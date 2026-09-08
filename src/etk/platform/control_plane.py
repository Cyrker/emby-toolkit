from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit

import httpx

ControlPlaneCertainty = Literal["confirmed-no-effect", "unknown", "observed-complete"]


class ControlPlaneError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        certainty: ControlPlaneCertainty = "unknown",
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.certainty = certainty
        self.request_id = request_id


class ControlPlaneClient:
    """Authenticated client for the narrow, pre-running Docker control plane."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token_file: Path | None = None,
        transport: httpx.BaseTransport | None = None,
        track_legacy_effects: bool = False,
    ) -> None:
        self._base_url = str(base_url or os.environ.get("ETK_CONTROL_PLANE_URL") or "").strip().rstrip("/")
        self._token_file = token_file or Path(
            os.environ.get(
                "ETK_CONTROL_PLANE_TOKEN_FILE",
                "/run/etk-control-plane/control-plane.token",
            )
        )
        self._transport = transport
        self._track_legacy_effects = bool(track_legacy_effects)

    def restart_service(self, target_service: str, *, request_id: str | None = None) -> dict[str, Any]:
        return self._command(
            "restart",
            {"target_service": str(target_service)},
            request_id=request_id,
            timeout_seconds=180,
        )

    def install_emby_plugin(
        self,
        *,
        emby_url: str,
        emby_token: str,
        etk_origin: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._command(
            "plugin-install",
            {
                "emby_url": str(emby_url),
                "emby_token": str(emby_token),
                "etk_origin": str(etk_origin),
            },
            request_id=request_id,
            timeout_seconds=600,
        )

    def update_service(
        self,
        target_service: str,
        *,
        strategy: str | None = None,
        helper_image: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if strategy is not None or helper_image is not None:
            raise ControlPlaneError(
                "control-plane rejects caller-provided update strategy or helper image",
                certainty="confirmed-no-effect",
                request_id=request_id,
            )
        return self._command(
            "update",
            {"target_service": str(target_service)},
            request_id=request_id,
            timeout_seconds=1800,
        )

    def _command(
        self,
        action: str,
        payload: dict[str, Any],
        *,
        request_id: str | None,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        self._validate_base_url()
        inherited_request_id = _current_job_id() if self._track_legacy_effects and request_id is None else None
        resolved_request_id = str(request_id or inherited_request_id or "").strip()
        if not resolved_request_id:
            raise ControlPlaneError(
                "control-plane command requires a persistent Job identity",
                certainty="confirmed-no-effect",
            )
        try:
            token = self._token_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ControlPlaneError(
                "control-plane token is unavailable",
                certainty="confirmed-no-effect",
                request_id=resolved_request_id,
            ) from exc
        if not token:
            raise ControlPlaneError(
                "control-plane token is empty",
                certainty="confirmed-no-effect",
                request_id=resolved_request_id,
            )

        command = {
            "request_id": resolved_request_id,
            "action": action,
            "payload": payload,
        }
        headers = {"authorization": f"Bearer {token}"}
        effect_label = _effect_label(action, payload)

        with httpx.Client(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout_seconds),
            transport=self._transport,
            trust_env=False,
        ) as client:
            try:
                prepared = _post_command(
                    client,
                    "/v1/commands/prepare",
                    headers=headers,
                    command=command,
                    default_certainty="confirmed-no-effect",
                    allow_unsuccessful=True,
                )
            except ControlPlaneError:
                raise
            except (httpx.HTTPError, ValueError) as exc:
                raise ControlPlaneError(
                    f"control-plane prepare failed: {exc}",
                    certainty="confirmed-no-effect",
                    request_id=resolved_request_id,
                ) from exc

            if _certainty(prepared) == "observed-complete":
                return prepared
            prepared_stage = str(prepared.get("stage") or "")
            if prepared_stage not in {"prepared", "dispatched", "failed-unknown"}:
                raise ControlPlaneError(
                    str(prepared.get("message") or "control-plane command was not prepared"),
                    certainty=_certainty(prepared),
                    request_id=resolved_request_id,
                )

            if self._track_legacy_effects:
                _record_effect_started(effect_label)
            try:
                result = _post_command(
                    client,
                    "/v1/commands/execute",
                    headers=headers,
                    command=command,
                    default_certainty="unknown",
                )
            except ControlPlaneError as exc:
                if exc.certainty == "confirmed-no-effect":
                    if self._track_legacy_effects:
                        _record_effect_abandoned(effect_label)
                elif exc.certainty == "observed-complete":
                    if self._track_legacy_effects:
                        _record_effect_committed(effect_label)
                raise
            except (httpx.HTTPError, ValueError) as exc:
                status = _query_command(client, resolved_request_id, headers=headers)
                certainty = _certainty(status) if status is not None else "unknown"
                if certainty == "observed-complete" and status is not None:
                    if self._track_legacy_effects:
                        _record_effect_committed(effect_label)
                    return status
                if certainty == "confirmed-no-effect":
                    if self._track_legacy_effects:
                        _record_effect_abandoned(effect_label)
                raise ControlPlaneError(
                    f"control-plane command failed: {exc}",
                    certainty=certainty,
                    request_id=resolved_request_id,
                ) from exc

        certainty = _certainty(result)
        if certainty != "observed-complete":
            if certainty == "confirmed-no-effect":
                if self._track_legacy_effects:
                    _record_effect_abandoned(effect_label)
            raise ControlPlaneError(
                str(result.get("message") or "control-plane command outcome is not confirmed"),
                certainty=certainty,
                request_id=resolved_request_id,
            )
        if self._track_legacy_effects:
            _record_effect_committed(effect_label)
        return result

    def _validate_base_url(self) -> None:
        parsed = urlsplit(self._base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ControlPlaneError(
                "ETK_CONTROL_PLANE_URL is not configured",
                certainty="confirmed-no-effect",
            )


def _current_job_id() -> str | None:
    import task_manager

    return task_manager.current_persistent_execution_job_id()


def _post_command(
    client: httpx.Client,
    path: str,
    *,
    headers: Mapping[str, str],
    command: Mapping[str, Any],
    default_certainty: ControlPlaneCertainty,
    allow_unsuccessful: bool = False,
) -> dict[str, Any]:
    response = client.post(path, headers=headers, json=command)
    result = response.json()
    if not isinstance(result, dict):
        raise ValueError("control-plane response is not an object")
    if response.is_error or (not allow_unsuccessful and not result.get("ok")):
        raise ControlPlaneError(
            str(result.get("message") or f"control-plane returned HTTP {response.status_code}"),
            certainty=_certainty(result, default=default_certainty),
            request_id=str(command.get("request_id") or "") or None,
        )
    return result


def _query_command(
    client: httpx.Client,
    request_id: str,
    *,
    headers: Mapping[str, str],
) -> dict[str, Any] | None:
    try:
        response = client.get(f"/v1/commands/{request_id}", headers=headers)
        result = response.json()
    except (httpx.HTTPError, ValueError):
        return None
    return result if isinstance(result, dict) and not response.is_error else None


def _certainty(
    result: Mapping[str, Any] | None,
    *,
    default: ControlPlaneCertainty = "unknown",
) -> ControlPlaneCertainty:
    value = str((result or {}).get("certainty") or "")
    if value in {"confirmed-no-effect", "unknown", "observed-complete"}:
        return cast(ControlPlaneCertainty, value)
    return default


def _effect_label(action: str, payload: Mapping[str, Any]) -> str:
    if action == "restart":
        return f"control-plane:restart:{str(payload.get('target_service') or 'service')}"
    if action == "update":
        return f"control-plane:update:{str(payload.get('target_service') or 'service')}"
    return "control-plane:plugin-install"


def _record_effect_started(label: str) -> None:
    import task_manager

    task_manager.record_persistent_effect_started(label)


def _record_effect_committed(label: str) -> None:
    import task_manager

    task_manager.record_persistent_effect_committed(label)


def _record_effect_abandoned(label: str) -> None:
    import task_manager

    task_manager.record_persistent_effect_abandoned(label)


__all__ = ["ControlPlaneCertainty", "ControlPlaneClient", "ControlPlaneError"]
