from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class AssetVersionSlot:
    asset_details: Mapping[str, Any]
    emby_item_id: str
    path: str = ""
    file_sha1: str = ""
    file_pickcode: str = ""


@dataclass(frozen=True, slots=True)
class AlignedAssetVersionArrays:
    asset_details: tuple[dict[str, Any], ...]
    emby_item_ids: tuple[str, ...]
    file_sha1s: tuple[str, ...]
    file_pickcodes: tuple[str, ...]


def _normalized_slot_path(value: object) -> str:
    return str(value or "").strip().replace("\\", "/").rstrip("/").casefold()


def align_asset_version_slots(slots: Iterable[AssetVersionSlot]) -> AlignedAssetVersionArrays:
    """Project ordered slots into four equal legacy arrays without fingerprint deduplication.

    A repeated item/path slot keeps its first asset and may fill missing fingerprints from
    the duplicate. Equal fingerprints on distinct slots remain repeated positional values.
    """
    assets: list[dict[str, Any]] = []
    emby_item_ids: list[str] = []
    file_sha1s: list[str] = []
    file_pickcodes: list[str] = []
    slot_indexes: dict[tuple[str, str], int] = {}

    for slot in slots:
        asset = dict(slot.asset_details)
        item_id = str(slot.emby_item_id or "").strip()
        path = slot.path or str(asset.get("path") or "")
        identity = (item_id, _normalized_slot_path(path))
        sha1 = str(slot.file_sha1 or "").strip()
        pickcode = str(slot.file_pickcode or "").strip()

        existing_index = slot_indexes.get(identity)
        if existing_index is not None:
            if not file_sha1s[existing_index] and sha1:
                file_sha1s[existing_index] = sha1
            if not file_pickcodes[existing_index] and pickcode:
                file_pickcodes[existing_index] = pickcode
            continue

        slot_indexes[identity] = len(assets)
        assets.append(asset)
        emby_item_ids.append(item_id)
        file_sha1s.append(sha1)
        file_pickcodes.append(pickcode)

    return AlignedAssetVersionArrays(
        asset_details=tuple(assets),
        emby_item_ids=tuple(emby_item_ids),
        file_sha1s=tuple(file_sha1s),
        file_pickcodes=tuple(file_pickcodes),
    )
