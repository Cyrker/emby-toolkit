from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

import httpx

from etk.domains.p115.auth_directory import P115CredentialStore
from etk.domains.p115.directory_transport import HttpxP115DirectoryTreeGateway
from etk.integrations import ProviderDisposition, ProviderResponse, SharedCenterPort
from etk.platform import Settings
from etk.platform.jobs.json_payload import canonical_json

from .execution import SharingDeliveryOutcome
from .operations import SharingOperation
from .repository import PostgresSharingOperationRepository
from .virtual_import import (
    VirtualImportBundle,
    VirtualImportService,
    freeze_virtual_import_source,
    virtual_import_state_hash,
)

_SHA1 = re.compile(r"[A-F0-9]{40}")
_SIGN_CHECK = re.compile(r"[0-9]{1,20}-[0-9]{1,20}")
_MAX_PAGES = 100
_PAGE_SIZE = 1000


@dataclass(frozen=True, slots=True)
class RapidUploadOutcome:
    status: str
    sign_key: str = ""
    sign_check: str = ""
    message: str = ""


@dataclass(frozen=True, slots=True)
class RapidTransferReadback:
    status: str
    receipts: tuple[dict[str, Any], ...] = ()
    missing_count: int = 0
    ambiguous_count: int = 0


class P115RapidTransferGateway(Protocol):
    def upload_init(
        self,
        identity: Mapping[str, Any],
        file: Mapping[str, Any],
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome: ...

    def readback(self, identity: Mapping[str, Any]) -> RapidTransferReadback: ...


class NativeP115RapidTransferGateway:
    """Account-bound Rapid v2 init/finalize plus unique parent-directory readback."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        target_cid: Callable[[], str],
        client_factory: Callable[[], httpx.Client] | None = None,
        cookie_client_factory: Callable[..., object] | None = None,
        directory: HttpxP115DirectoryTreeGateway | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._target_cid = target_cid
        self._client_factory = client_factory or (
            lambda: httpx.Client(timeout=max(1.0, timeout_seconds), trust_env=False)
        )
        self._cookie_client_factory = cookie_client_factory or _default_cookie_client
        self._directory = directory or HttpxP115DirectoryTreeGateway(
            credentials=credentials,
            priority=priority,
            client_factory=lambda **kwargs: httpx.Client(**kwargs),
            timeout_seconds=timeout_seconds,
        )

    def upload_init(
        self,
        identity: Mapping[str, Any],
        file: Mapping[str, Any],
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome:
        account_id, backend, target_cid, snapshot = self._identity(identity)
        sha1 = _sha1(file.get("sha1"))
        size = _positive_int(file.get("size"), "file size")
        file_name = _text(file.get("file_name"), "file name", limit=512)
        preid = _sha1(file.get("preid"))
        data = {
            "file_name": file_name,
            "file_size": size,
            "fileid": sha1,
            "preid": preid,
            "target": f"U_1_{target_cid}",
        }
        if sign_key or sign_val:
            if not sign_key or not _SHA1.fullmatch(str(sign_val or "").strip().upper()):
                raise ValueError("rapid signature is incomplete")
            data.update({"sign_key": sign_key, "sign_val": str(sign_val).strip().upper()})
        if backend == "openapi":
            with self._client_factory() as client:
                response = client.post(
                    "https://proapi.115.com/open/upload/init",
                    headers={"Authorization": f"Bearer {snapshot.access_token}"},
                    data=data,
                )
                response.raise_for_status()
                payload = response.json()
        else:
            client = self._cookie_client_factory(snapshot.cookie, app=snapshot.app_type)
            method = getattr(client, "upload_init", None)
            if not callable(method):
                raise RuntimeError("p115client cookie upload_init is unavailable")
            payload = method(
                {
                    "filename": file_name,
                    "filesize": size,
                    "fileid": sha1,
                    "preid": preid,
                    "target": f"U_1_{target_cid}",
                    **({"sign_key": sign_key, "sign_val": str(sign_val).strip().upper()} if sign_key else {}),
                }
            )
        self._assert_identity(identity)
        return _rapid_upload_outcome(payload)

    def readback(self, identity: Mapping[str, Any]) -> RapidTransferReadback:
        account_id, backend, target_cid, _snapshot = self._identity(identity)
        files = _identity_files(identity)
        client = self._directory.open(expected_account_id=account_id, expected_backend=backend)
        if client is None:
            raise RuntimeError("115 transfer readback client is unavailable")
        candidates: list[Mapping[str, Any]] = []
        offset = 0
        for _page in range(_MAX_PAGES):
            response = client.list_page(cid=target_cid, limit=_PAGE_SIZE, offset=offset)
            if response.get("state") is not True or response.get("_etk_authoritative_list_payload") is not True:
                raise RuntimeError("115 transfer readback was not authoritative")
            items = _page_items(response)
            candidates.extend(items)
            offset += len(items)
            total = _nonnegative_int(response.get("count") or response.get("total"))
            if not items or (total and offset >= total) or len(items) < _PAGE_SIZE:
                break
        else:
            raise RuntimeError("115 transfer readback exceeded page limit")
        self._assert_identity(identity)

        receipts: list[dict[str, Any]] = []
        missing = 0
        ambiguous = 0
        for file in files:
            sha1 = _sha1(file.get("sha1"))
            size = _positive_int(file.get("size"), "file size")
            matches = [item for item in candidates if _item_sha1(item) == sha1 and _item_size(item) == size]
            if str(file.get("source_kind") or "").strip() in {"playback", "speedtest"}:
                expected_name = _text(file.get("file_name"), "playback file name", limit=512)
                matches = [item for item in matches if _item_name(item) == expected_name]
            if not matches:
                missing += 1
                continue
            if len(matches) != 1:
                ambiguous += 1
                continue
            item = matches[0]
            fid = _item_id(item)
            pick_code = _item_pick_code(item)
            if not fid or not pick_code:
                ambiguous += 1
                continue
            receipts.append(
                {
                    "fid": fid,
                    "pick_code": pick_code,
                    "sha1": sha1,
                    "size": size,
                    "file_name": _item_name(item) or str(file.get("file_name") or ""),
                    "target_cid": target_cid,
                    "backend": backend,
                    "account_id": account_id,
                    "evidence": "authoritative_parent_listing",
                }
            )
        status = "confirmed" if len(receipts) == len(files) else ("ambiguous" if ambiguous else "partial")
        return RapidTransferReadback(status, tuple(receipts), missing, ambiguous)

    def _identity(self, value: Mapping[str, Any]) -> tuple[str, str, str, Any]:
        account_id = _text(value.get("account_id"), "account id", limit=128)
        backend = _text(value.get("backend"), "backend", limit=16).lower()
        target_cid = _text(value.get("target_cid"), "target cid", limit=128)
        if backend not in {"openapi", "cookie"}:
            raise ValueError("rapid backend is invalid")
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            raise RuntimeError("115 account changed during rapid transfer")
        if _rapid_backend(snapshot, self._priority()) != backend:
            raise RuntimeError("115 backend changed during rapid transfer")
        if _text(self._target_cid(), "115 save path cid", limit=128) != target_cid:
            raise RuntimeError("115 target changed during rapid transfer")
        expected_credential = _hash64(value.get("credential_hash"), "credential hash")
        if _credential_fingerprint(snapshot, backend) != expected_credential:
            raise RuntimeError("115 credential changed during rapid transfer")
        return account_id, backend, target_cid, snapshot

    def _assert_identity(self, identity: Mapping[str, Any]) -> None:
        self._identity(identity)


class RapidTransferOperationDelivery:
    """One-checkpoint center lease/sign/rapid transfer with readback-only recovery."""

    def __init__(
        self,
        *,
        repository: PostgresSharingOperationRepository,
        virtual_imports: VirtualImportService,
        credentials: P115CredentialStore,
        settings: Callable[[], Settings],
        center: SharedCenterPort,
        p115: P115RapidTransferGateway,
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
            "transfer_center_source"
        )

    def request(
        self,
        operation: SharingOperation,
        *,
        checkpoint: Callable[[], None],
    ) -> SharingDeliveryOutcome:
        payload = _transfer_payload(operation)
        source = cast(Mapping[str, Any], payload["source"])
        intent = freeze_virtual_import_source(source)
        if intent.state_hash != _hash64(payload.get("source_state_hash"), "source state hash"):
            raise ValueError("center transfer source state changed")
        bundle = self._virtual_imports.prepare(intent.source)
        self._assert_virtual(payload)
        identity = self._prepare_identity(bundle, intent.state_hash, payload)
        identity["operation_id"] = str(operation.id)
        if operation.claim_token is None:
            raise RuntimeError("sharing transfer operation is not claimed")
        self._repository.save_prewrite_state(operation.id, operation.claim_token, identity)

        existing = self._p115.readback(identity)
        if existing.status == "confirmed":
            checkpoint()
            identity["phase"] = "confirmed_existing"
            self._save_request_state(operation, identity)
            return SharingDeliveryOutcome.confirmed(_transfer_result(identity, existing, existing=True))

        checkpoint()
        identity["phase"] = "lease_pending"
        self._save_request_state(operation, identity)
        lease_response = self._acquire_lease(identity)
        if lease_response.disposition is ProviderDisposition.REJECTED:
            identity["phase"] = "lease_rejected"
            self._save_request_state(operation, identity)
            return SharingDeliveryOutcome.rejected({"type": "rapid_transfer_lease_rejected"})
        if lease_response.disposition is not ProviderDisposition.CONFIRMED or not isinstance(
            lease_response.payload, Mapping
        ):
            return SharingDeliveryOutcome.indeterminate({"type": "rapid_transfer_lease_unknown"})
        lease = dict(lease_response.payload)
        if lease.get("allow") is False and lease.get("deferred") is not True:
            identity["phase"] = "lease_rejected"
            self._save_request_state(operation, identity)
            return SharingDeliveryOutcome.rejected({"type": "rapid_transfer_lease_rejected"})
        if lease.get("deferred") is True:
            return SharingDeliveryOutcome.indeterminate({"type": "rapid_transfer_lease_deferred"})
        lease_id = _lease_id(lease)
        identity.update(phase="lease_confirmed", lease_id=lease_id)
        self._save_request_state(operation, identity)
        for index, file in enumerate(_identity_files(identity)):
            identity.update(phase="upload_init_pending", current_file_index=index)
            self._save_request_state(operation, identity)
            initial = self._p115.upload_init(identity, file)
            if initial.status == "sign_required":
                signed = self._center_sign(operation, identity, file, initial, index=index, lease_id=lease_id)
                if signed.status != "confirmed":
                    return SharingDeliveryOutcome.indeterminate(
                        {"type": "rapid_signed_finalize_not_confirmed", "message": signed.message[:500]}
                    )
            elif initial.status == "rejected":
                identity.update(phase="upload_rejected", current_file_index=index)
                self._save_request_state(operation, identity)
                return SharingDeliveryOutcome.rejected(
                    {"type": "rapid_plain_upload_rejected", "message": initial.message[:500]}
                )
            elif initial.status != "confirmed":
                return SharingDeliveryOutcome.indeterminate(
                    {"type": "rapid_upload_init_not_confirmed", "message": initial.message[:500]}
                )
            completed = list(identity.get("completed_file_indexes") or [])
            if index not in completed:
                completed.append(index)
            identity.update(phase="file_confirmed", completed_file_indexes=completed)
            self._save_request_state(operation, identity)
        identity["phase"] = "target_readback"
        self._save_request_state(operation, identity)
        readback = self._p115.readback(identity)
        if readback.status != "confirmed":
            return SharingDeliveryOutcome.indeterminate(
                {
                    "type": "rapid_transfer_readback_incomplete",
                    "missing_count": readback.missing_count,
                    "ambiguous_count": readback.ambiguous_count,
                }
            )
        identity["phase"] = "confirmed"
        self._save_request_state(operation, identity)
        return SharingDeliveryOutcome.confirmed(_transfer_result(identity, readback, existing=False))

    def readback(self, operation: SharingOperation) -> SharingDeliveryOutcome:
        identity = operation.state if isinstance(operation.state, Mapping) else {}
        if str(identity.get("kind") or "") != "rapid_transfer":
            return SharingDeliveryOutcome.indeterminate({"type": "rapid_transfer_readback_identity_missing"})
        try:
            self._assert_virtual(_transfer_payload(operation))
        except (TypeError, ValueError):
            return SharingDeliveryOutcome.indeterminate({"type": "virtual_import_state_changed_after_request"})
        readback = self._p115.readback(identity)
        if readback.status == "confirmed":
            return SharingDeliveryOutcome.confirmed(_transfer_result(identity, readback, existing=False))
        return SharingDeliveryOutcome.retryable(
            {
                "type": "rapid_transfer_readback_incomplete",
                "missing_count": readback.missing_count,
                "ambiguous_count": readback.ambiguous_count,
            }
        )

    def _prepare_identity(
        self,
        bundle: VirtualImportBundle,
        source_state_hash: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        snapshot = self._credentials.load()
        account_id = _text(snapshot.account_id, "115 account id", limit=128)
        settings = self._settings()
        backend = _rapid_backend(snapshot, settings.p115_api_priority)
        target_cid = _text(settings.p115_save_path_cid, "115 save path cid", limit=128)
        files = tuple(_transfer_file(item) for item in bundle.files)
        credential_hash = _credential_fingerprint(snapshot, backend)
        manifest_hash = hashlib.sha256(
            canonical_json({"files": list(files)}, field="rapid transfer file manifest").encode()
        ).hexdigest()
        return {
            "schema_version": 1,
            "kind": "rapid_transfer",
            "account_id": account_id,
            "backend": backend,
            "credential_hash": credential_hash,
            "target_cid": target_cid,
            "source_kind": bundle.source_kind,
            "source_id": bundle.source_id,
            "source_state_hash": source_state_hash,
            "files": list(files),
            "files_manifest_hash": manifest_hash,
            "virtual_id": _optional_positive_int(payload.get("virtual_id")),
            "virtual_state_hash": str(payload.get("virtual_state_hash") or ""),
            "phase": "prepared",
            "lease_id": "",
            "sign_job_ids": [],
            "completed_file_indexes": [],
        }

    def _assert_virtual(self, payload: Mapping[str, Any]) -> None:
        virtual_id = _optional_positive_int(payload.get("virtual_id"))
        if virtual_id is None:
            return
        expected = _hash64(payload.get("virtual_state_hash"), "virtual state hash")
        row = self._virtual_imports.get(virtual_id)
        if row is None or virtual_import_state_hash(row) != expected:
            raise ValueError("virtual import changed before promotion")

    def _acquire_lease(self, identity: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        files = _identity_files(identity)
        first = files[0]
        response = self._center.acquire_transfer_lease(
            {
                "source_kind": first.get("source_kind") or identity.get("source_kind"),
                "source_id": first.get("source_id") or identity.get("source_id"),
                "sha1": first["sha1"],
                "transfer_mode": "rapid",
                "request_meta_json": {
                    "client_gate": "etk_native_rapid_transfer_v1",
                    "file_count": len(files),
                    "files_manifest_hash": identity["files_manifest_hash"],
                    "account_id": identity["account_id"],
                    "backend": identity["backend"],
                    "target_cid": identity["target_cid"],
                    "operation_id": identity["operation_id"],
                },
            }
        )
        return response

    def _center_sign(
        self,
        operation: SharingOperation,
        identity: Mapping[str, Any],
        file: Mapping[str, Any],
        initial: RapidUploadOutcome,
        *,
        index: int,
        lease_id: str,
    ) -> RapidUploadOutcome:
        if not initial.sign_key or not _SIGN_CHECK.fullmatch(initial.sign_check):
            return RapidUploadOutcome("indeterminate", message="rapid sign challenge is incomplete")
        mutable_identity = cast(dict[str, Any], identity)
        mutable_identity.update(phase="sign_job_create_pending", current_file_index=index)
        self._save_request_state(operation, mutable_identity)
        created = self._center.create_rapid_sign_job(
            {
                "source_kind": file.get("source_kind") or identity.get("source_kind"),
                "source_id": file.get("source_id") or identity.get("source_id"),
                "sha1": file["sha1"],
                "size": file["size"],
                "file_name": file["file_name"],
                "preid": file["preid"],
                "backend": identity["backend"],
                "sign_key": initial.sign_key,
                "sign_check": initial.sign_check,
                "request_meta_json": {"target_cid": identity["target_cid"], "lease_id": lease_id},
            }
        )
        if created.disposition is not ProviderDisposition.CONFIRMED or not isinstance(created.payload, Mapping):
            return RapidUploadOutcome("indeterminate", message="rapid sign job creation was not confirmed")
        created_payload = dict(created.payload)
        job = dict(created_payload["job"]) if isinstance(created_payload.get("job"), Mapping) else {}
        job_id = str(created_payload.get("job_id") or job.get("job_id") or "").strip()
        if not job_id:
            return RapidUploadOutcome("indeterminate", message="rapid sign job id is missing")
        job_ids = list(mutable_identity.get("sign_job_ids") or [])
        if job_id not in job_ids:
            job_ids.append(job_id)
        mutable_identity.update(phase="sign_job_waiting", sign_job_ids=job_ids)
        self._save_request_state(operation, mutable_identity)
        waited = self._center.wait_rapid_sign_job(job_id, timeout=75)
        if waited.disposition is not ProviderDisposition.CONFIRMED or not isinstance(waited.payload, Mapping):
            return RapidUploadOutcome("indeterminate", message="rapid sign job readback was not confirmed")
        waited_payload = dict(waited.payload)
        waited_job = dict(waited_payload["job"]) if isinstance(waited_payload.get("job"), Mapping) else {}
        status = str(waited_payload.get("status") or waited_job.get("status") or "").strip()
        sign_val = str(waited_payload.get("sign_val") or waited_job.get("sign_val") or "").strip().upper()
        if status != "done" or not _SHA1.fullmatch(sign_val):
            return RapidUploadOutcome("indeterminate", message="rapid sign job did not produce a valid signature")
        mutable_identity["phase"] = "signed_finalize_pending"
        self._save_request_state(operation, mutable_identity)
        return self._p115.upload_init(identity, file, sign_key=initial.sign_key, sign_val=sign_val)

    def _save_request_state(self, operation: SharingOperation, identity: Mapping[str, Any]) -> None:
        if operation.claim_token is None:
            raise RuntimeError("sharing transfer operation is not claimed")
        self._repository.save_request_state(operation.id, operation.claim_token, identity)


def _default_cookie_client(cookie: str, *, app: str) -> object:
    from p115client import P115Client

    return P115Client(cookie, app=app)


def _rapid_upload_outcome(value: Any) -> RapidUploadOutcome:
    if not isinstance(value, Mapping):
        return RapidUploadOutcome("indeterminate", message="115 upload/init returned invalid JSON")
    data = dict(value["data"]) if isinstance(value.get("data"), Mapping) else {}
    status = str(value.get("status") or data.get("status") or "").strip().lower()
    state = value.get("state") is True or value.get("success") is True
    reuse = value.get("reuse") is True or str(value.get("reuse") or "").strip().lower() == "true"
    if reuse or (state and status in {"2", "success", "done"}):
        return RapidUploadOutcome("confirmed")
    if status == "7":
        sign_key = str(value.get("sign_key") or data.get("sign_key") or "").strip()
        sign_check = str(value.get("sign_check") or data.get("sign_check") or "").strip()
        if sign_key and _SIGN_CHECK.fullmatch(sign_check):
            return RapidUploadOutcome("sign_required", sign_key=sign_key, sign_check=sign_check)
        return RapidUploadOutcome("indeterminate", message="115 status=7 response omitted sign challenge")
    message = str(value.get("error_msg") or value.get("message") or f"115 upload/init status={status or 'unknown'}")
    return RapidUploadOutcome("rejected" if status == "1" else "indeterminate", message=message[:500])


def _transfer_payload(operation: SharingOperation) -> dict[str, Any]:
    request = operation.request if isinstance(operation.request, Mapping) else {}
    payload = request.get("payload")
    if not isinstance(payload, Mapping) or not isinstance(payload.get("source"), Mapping):
        raise ValueError("center transfer payload is invalid")
    return dict(payload)


def _transfer_file(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "sha1": _sha1(value.get("sha1")),
        "size": _positive_int(value.get("size"), "file size"),
        "file_name": _text(value.get("file_name") or value.get("name"), "file name", limit=512),
        "preid": _sha1(value.get("preid") or value.get("pre_sha1") or value.get("pre_sha1_128k")),
        "source_kind": _text(value.get("source_kind"), "file source kind", limit=64),
        "source_id": _text(value.get("source_id") or value.get("source_ref_id"), "file source id", limit=255),
    }


def _identity_files(value: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    files = value.get("files")
    if not isinstance(files, list) or not files or any(not isinstance(item, Mapping) for item in files):
        raise ValueError("rapid transfer file manifest is invalid")
    return tuple(_transfer_file(cast(Mapping[str, Any], item)) for item in files)


def _transfer_result(
    identity: Mapping[str, Any],
    readback: RapidTransferReadback,
    *,
    existing: bool,
) -> dict[str, Any]:
    return {
        "authoritative_readback": True,
        "transfer_confirmed": True,
        "existing": existing,
        "lease_id": str(identity.get("lease_id") or ""),
        "source_kind": str(identity.get("source_kind") or ""),
        "source_id": str(identity.get("source_id") or ""),
        "account_id": str(identity.get("account_id") or ""),
        "backend": str(identity.get("backend") or ""),
        "target_cid": str(identity.get("target_cid") or ""),
        "files_manifest_hash": str(identity.get("files_manifest_hash") or ""),
        "rapid_sign_job_ids": list(identity.get("sign_job_ids") or []),
        "receipts": list(readback.receipts),
        "success_count": len(readback.receipts),
        "total_count": len(_identity_files(identity)),
    }


def _rapid_backend(snapshot: Any, priority: object) -> str:
    normalized = str(priority or "openapi").strip().lower()
    backend = "cookie" if normalized == "cookie" and snapshot.has_cookie else "openapi" if snapshot.has_openapi else ""
    if not backend and snapshot.has_cookie:
        backend = "cookie"
    if not backend:
        raise RuntimeError("115 rapid transfer credential is unavailable")
    return backend


def _credential_fingerprint(snapshot: Any, backend: str) -> str:
    credential = snapshot.access_token if backend == "openapi" else snapshot.cookie
    app_type = "" if backend == "openapi" else str(snapshot.app_type or "")
    return hashlib.sha256(f"{backend}\0{app_type}\0{credential}".encode()).hexdigest()


def _lease_id(payload: Mapping[str, Any]) -> str:
    nested = dict(payload["lease"]) if isinstance(payload.get("lease"), Mapping) else {}
    value = str(payload.get("lease_id") or nested.get("lease_id") or "").strip()
    if not value or len(value) > 255:
        raise RuntimeError("Shared Center transfer lease id is missing")
    return value


def _page_items(value: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for container in (value, value.get("data") if isinstance(value.get("data"), Mapping) else {}):
        if not isinstance(container, Mapping):
            continue
        for key in ("data", "list", "items", "files"):
            items = container.get(key)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, Mapping)]
    return []


def _item_sha1(value: Mapping[str, Any]) -> str:
    raw = str(value.get("sha1") or value.get("file_sha1") or value.get("sha") or "").strip().upper()
    return raw if _SHA1.fullmatch(raw) else ""


def _item_size(value: Mapping[str, Any]) -> int:
    return _nonnegative_int(value.get("size") or value.get("file_size") or value.get("s"))


def _item_id(value: Mapping[str, Any]) -> str:
    return str(value.get("fid") or value.get("file_id") or value.get("id") or "").strip()


def _item_pick_code(value: Mapping[str, Any]) -> str:
    return str(value.get("pick_code") or value.get("pickcode") or value.get("pc") or "").strip()


def _item_name(value: Mapping[str, Any]) -> str:
    return str(value.get("file_name") or value.get("name") or value.get("n") or "").strip()


def _sha1(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not _SHA1.fullmatch(normalized):
        raise ValueError("rapid transfer sha1 is invalid")
    return normalized


def _hash64(value: Any, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"{field} is invalid")
    return normalized


def _text(value: Any, field: str, *, limit: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > limit:
        raise ValueError(f"{field} is invalid")
    return normalized


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if normalized < 1:
        raise ValueError(f"{field} is invalid")
    return normalized


def _nonnegative_int(value: Any) -> int:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _optional_positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return _positive_int(value, "virtual id")


__all__ = [
    "NativeP115RapidTransferGateway",
    "P115RapidTransferGateway",
    "RapidTransferOperationDelivery",
    "RapidTransferReadback",
    "RapidUploadOutcome",
]
