from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from etk.domains.media.effects import PostgresMediaEffectLedger
from etk.domains.system.standalone import StandaloneSettingsRepository


class ConfiguredP115IntroSyncFeature:
    """Compatibility facade for the retired shared-intro export feature."""

    def __init__(self, settings: StandaloneSettingsRepository) -> None:
        del settings

    def enabled(self) -> bool:
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
        del sha1, chapters, file_name, reason, cache_revision


__all__ = ["ConfiguredP115IntroSyncFeature", "PersistentP115IntroSnapshotExporter"]
