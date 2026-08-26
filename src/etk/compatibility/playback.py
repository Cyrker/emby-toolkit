from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from etk.domains.p115.playback import PlaybackMode, PlaybackPrepareRequest


class PlaybackCompatibilityService(Protocol):
    def is_pool_enabled(self) -> bool: ...

    def has_usable_pool(self, user_id: str = "", *, own_account_only: bool = False) -> bool: ...

    def prepare_play(
        self,
        request: PlaybackPrepareRequest,
        *,
        force_new: bool = False,
        failed_remote_pick_code: str = "",
    ) -> Any | None: ...

    def resolve_lease(self, lease_id: str, user_agent: str) -> str: ...

    def resolve_virtual_play(self, virtual_info: Mapping[str, Any], **context: Any) -> str: ...

    def release_for_stop(self, data: Mapping[str, Any]) -> int: ...

    def cleanup(self) -> int: ...


class PersistentPlaybackGateway(Protocol):
    def pool_enabled(self) -> bool: ...

    def pool_available(self, user_id: str = "", *, own_account_only: bool = False) -> bool: ...

    def prepare_pool(self, source_pick_code: str, **context: Any) -> dict[str, Any]: ...

    def pool_direct_url(self, play_result: Mapping[str, Any], *, user_agent: str = "") -> str: ...

    def prepare_copy(self, source_pick_code: str, **context: Any) -> str: ...

    def resolve_virtual(self, virtual_info: Mapping[str, Any], **context: Any) -> str: ...

    def release_for_stop(self, data: Mapping[str, Any]) -> int: ...

    def cleanup(self) -> int: ...


class NativePlaybackCompatibilityGateway:
    """Keep legacy call signatures while routing active playback state to P6.9."""

    def __init__(self, service: Callable[[], PlaybackCompatibilityService]) -> None:
        self._service_provider = service

    def pool_enabled(self) -> bool:
        return self._service_provider().is_pool_enabled()

    def pool_available(self, user_id: str = "", *, own_account_only: bool = False) -> bool:
        return self._service_provider().has_usable_pool(
            str(user_id or ""),
            own_account_only=bool(own_account_only),
        )

    def prepare_pool(self, source_pick_code: str, **context: Any) -> dict[str, Any]:
        prepared = self._service_provider().prepare_play(_request(PlaybackMode.POOL, source_pick_code, context))
        if prepared is None:
            return {}
        return {
            "_native_playback": True,
            "pick_code": prepared.artifact.remote_pick_code,
            "lease_id": str(prepared.lease.id),
            "artifact_id": str(prepared.artifact.id),
            "session": {
                "lease_id": str(prepared.lease.id),
                "play_session_id": prepared.lease.play_session_id,
                "item_id": prepared.lease.item_id,
                "user_id": prepared.lease.owner_user_id,
            },
        }

    def pool_direct_url(self, play_result: Mapping[str, Any], *, user_agent: str = "") -> str:
        if not bool(play_result.get("_native_playback")):
            return ""
        lease_id = str(play_result.get("lease_id") or (play_result.get("session") or {}).get("lease_id") or "").strip()
        if not lease_id:
            return ""
        return self._service_provider().resolve_lease(lease_id, str(user_agent or ""))

    def prepare_copy(self, source_pick_code: str, **context: Any) -> str:
        force_new = bool(context.pop("force_new", False))
        failed_remote_pick_code = str(context.pop("failed_pick_code", "") or "").strip()
        prepared = self._service_provider().prepare_play(
            _request(PlaybackMode.COPY, source_pick_code, context),
            force_new=force_new,
            failed_remote_pick_code=failed_remote_pick_code,
        )
        return str(prepared.artifact.remote_pick_code if prepared is not None else "")

    def resolve_virtual(self, virtual_info: Mapping[str, Any], **context: Any) -> str:
        return self._service_provider().resolve_virtual_play(virtual_info, **context)

    def release_for_stop(self, data: Mapping[str, Any]) -> int:
        return self._service_provider().release_for_stop(data)

    def cleanup(self) -> int:
        return self._service_provider().cleanup()


def _request(mode: PlaybackMode, source_pick_code: str, context: Mapping[str, Any]) -> PlaybackPrepareRequest:
    return PlaybackPrepareRequest(
        mode=mode,
        source_pick_code=str(source_pick_code or "").strip(),
        file_name=str(context.get("file_name") or ""),
        item_id=str(context.get("item_id") or ""),
        play_session_id=str(context.get("play_session_id") or ""),
        user_id=str(context.get("user_id") or ""),
        source=str(context.get("source") or "legacy-compatibility"),
        client_key=str(context.get("client_key") or ""),
        client_name=str(context.get("client_name") or ""),
        user_agent=str(context.get("user_agent") or ""),
        own_account_only=bool(context.get("own_account_only")),
    )


_LOCK = threading.RLock()
_GATEWAY: PersistentPlaybackGateway | None = None


def install_persistent_playback_gateway(gateway: PersistentPlaybackGateway) -> None:
    if gateway is None:
        raise TypeError("persistent playback gateway is required")
    global _GATEWAY
    with _LOCK:
        if _GATEWAY is not None and _GATEWAY is not gateway:
            raise RuntimeError("a different persistent playback gateway is already installed")
        _GATEWAY = gateway


def clear_persistent_playback_gateway(gateway: PersistentPlaybackGateway | None = None) -> None:
    global _GATEWAY
    with _LOCK:
        if gateway is None or _GATEWAY is gateway:
            _GATEWAY = None


def persistent_playback_gateway() -> PersistentPlaybackGateway | None:
    with _LOCK:
        return _GATEWAY


__all__ = [
    "NativePlaybackCompatibilityGateway",
    "PersistentPlaybackGateway",
    "PlaybackCompatibilityService",
    "clear_persistent_playback_gateway",
    "install_persistent_playback_gateway",
    "persistent_playback_gateway",
]
