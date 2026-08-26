from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from etk.compatibility.background import spawn

logger = logging.getLogger(__name__)


class LegacyP115IntroSyncFeature:
    """Compatibility read seam for the P9-owned shared-resource feature flags."""

    def enabled(self) -> bool:
        try:
            from database import settings_db

            config = settings_db.get_shared_resource_config() or {}
            return bool(config.get("p115_shared_resource_enabled")) and bool(config.get("p115_shared_intro_enabled"))
        except Exception:
            logger.debug("P115 shared intro feature lookup failed; treating it as disabled", exc_info=True)
            return False


class LegacyP115IntroSnapshotExporter:
    """Best-effort compatibility seam until P9 owns durable SharedCenter delivery."""

    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        snapshot = tuple(dict(chapter) for chapter in chapters)
        spawn(
            self._publish,
            sha1,
            snapshot,
            str(file_name or "").strip(),
            str(reason or "").strip(),
            cache_revision,
        )

    @staticmethod
    def _publish(
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None:
        try:
            from handler.shared_center_client import SharedCenterClient, shared_center_enabled

            if not LegacyP115IntroSyncFeature().enabled() or not shared_center_enabled():
                return
            SharedCenterClient().upload_intro_chapters(
                sha1,
                [dict(chapter) for chapter in chapters],
                file_name=file_name,
                reason=reason,
            )
        except Exception:
            logger.debug(
                "P115 intro snapshot compatibility export failed at cache revision %s",
                cache_revision,
                exc_info=True,
            )


__all__ = ["LegacyP115IntroSnapshotExporter", "LegacyP115IntroSyncFeature"]
