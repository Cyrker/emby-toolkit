from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.sharing.configuration import SharingConfigurationService
from etk.domains.sharing.intro_effects import SHARED_INTRO_EXPORT_EFFECT
from etk.domains.system.standalone import StandaloneSettingsRepository
from etk.platform.jobs.json_payload import canonical_json

logger = logging.getLogger(__name__)


class ConfiguredP115IntroSyncFeature:
    """Read the P9-owned shared-intro feature flags from native app settings."""

    def __init__(self, settings: StandaloneSettingsRepository) -> None:
        self._configuration = SharingConfigurationService(settings)

    def enabled(self) -> bool:
        try:
            config = self._configuration.load()
            return bool(config.get("p115_shared_resource_enabled")) and bool(config.get("p115_shared_intro_enabled"))
        except Exception:
            logger.debug("P115 shared intro feature lookup failed; treating it as disabled", exc_info=True)
            return False


class PersistentP115IntroSnapshotExporter:
    """Stage changed intro snapshots through the durable P9 media-effect path."""

    def __init__(self, ledger: PostgresMediaEffectLedger) -> None:
        self._ledger = ledger

    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        del cache_revision
        payload = {
            "sha1": str(sha1 or "").strip().upper(),
            "chapters": [dict(chapter) for chapter in chapters],
            "file_name": str(file_name or "").strip(),
            "reason": str(reason or "etk_intro_detection").strip(),
        }
        digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
        self._ledger.reserve(
            effect_key=f"p9:{SHARED_INTRO_EXPORT_EFFECT}:{payload['sha1']}:{digest}",
            effect_type=SHARED_INTRO_EXPORT_EFFECT,
            aggregate_type="media_asset",
            aggregate_id=payload["sha1"],
            payload=payload,
        )


__all__ = ["ConfiguredP115IntroSyncFeature", "PersistentP115IntroSnapshotExporter"]
