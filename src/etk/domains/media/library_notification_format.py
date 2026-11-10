from __future__ import annotations

from collections.abc import Mapping, Sequence
from html import escape
from itertools import groupby
from typing import Any
from urllib.parse import quote


def library_card(members: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    # 同一项的补齐/重试取最后一次结果，不重复计算集数和体积。
    ordered = sorted(members, key=lambda m: str(m.get("completed_at") or ""))
    unique = {str(m["item_id"]): m for m in ordered}
    if not unique:
        raise ValueError("library notification batch is empty")
    items = list(unique.values())
    latest = items[-1]
    is_movie = latest.get("item_type") == "Movie"
    failed = [m for m in items if not m.get("succeeded")]
    retry_successes: dict[int, list[Mapping[str, Any]]] = {}
    for member in items:
        retry_count = member.get("retry_success_count")
        if type(retry_count) is int and retry_count > 0:
            retry_successes.setdefault(retry_count, []).append(member)
    if failed:
        status = "⚠️ 部分待处理"
    elif len(items) == 1 and retry_successes:
        retry_count = next(iter(retry_successes))
        status = f"✅ 已成功入库（第 {retry_count} 次重刮成功）"
    else:
        status = "✨ 入库成功" if is_movie else "🔄 已更新"
    title = str(latest.get("title") or "未知媒体")
    if latest.get("year"):
        title += f" ({latest['year']})"
    lines = [f"{'🎬' if is_movie else '📺'} <b>{_html(title, 160)}</b> {status}", ""]
    refs = [(m.get("season"), m.get("episode")) for m in items]
    episodes = episode_ranges(refs)
    if episodes:
        lines.append(f"🎞️ <b>集数</b>: <code>{episodes}</code>")
    assets = {}
    for member in items:
        for asset in member.get("assets") or []:
            assets[str(asset.get("path") or asset.get("emby_item_id") or member["item_id"])] = asset
    sizes = [_asset_size(asset) for asset in assets.values()]
    known = sum(size > 0 for size in sizes)
    if known:
        suffix = "，部分体积未知" if known < len(assets) else ""
        lines.append(f"💾 <b>体积</b>: <code>{_size_text(sum(sizes))}（{len(assets)}个文件{suffix}）</code>")
    lines.append(f"⏰ <b>时间</b>: <code>{_html(latest.get('display_time'), 30)}</code>")
    overview = next((m.get("overview") for m in reversed(items) if m.get("overview")), "暂无剧情简介。")
    lines.append(f"📝 <b>剧情</b>: {_html(_clip(str(overview), 200), 320)}")
    if not (len(items) == 1 and not failed and retry_successes):
        for retry_count, recovered in sorted(retry_successes.items()):
            refs = episode_ranges([(m.get("season"), m.get("episode")) for m in recovered])
            if not refs:
                refs = str(recovered[0].get("title") or "媒体") if len(recovered) == 1 else f"{len(recovered)}项"
            lines.append(
                f"✅ <b>已成功入库（第 {retry_count} 次重刮成功）</b>: {_html(_clip(refs, 100), 120)}"
            )
    if failed:
        failed_refs = episode_ranges([(m.get("season"), m.get("episode")) for m in failed])
        lines.extend(["", f"⚠️ <b>待处理</b>: {_clip(failed_refs, 100) or str(len(failed)) + '项'}；请查看任务中心。"])
    photo = next((m.get("photo") for m in reversed(items) if m.get("photo")), None)
    caption = "\n".join(lines)
    if len(caption) > 1024:
        raise ValueError("library notification caption exceeds Telegram limit")
    return {"message": caption, "parse_mode": "HTML", "photo": photo}


def episode_ranges(refs: Sequence[tuple[Any, Any]]) -> str:
    valid = sorted({(s, e) for s, e in refs if type(s) is int and type(e) is int and s >= 0 and e >= 0})
    parts = []
    for season, group in groupby(valid, key=lambda pair: pair[0]):
        episodes = [pair[1] for pair in group]
        first = last = episodes[0]
        for number in episodes[1:] + [None]:
            if number is not None and number == last + 1:
                last = number
                continue
            parts.append(f"S{season:02d}E{first:02d}" + (f"–E{last:02d}" if last != first else ""))
            if number is not None:
                first = last = number
    result = ", ".join(parts)
    return result if len(result) <= 150 else ", ".join(parts[:3])[:100] + f" … 共{len(valid)}集"


def tmdb_photo(metadata: Mapping[str, Any]) -> str | None:
    path = str(metadata.get("backdrop_path") or metadata.get("poster_path") or "")
    if path.startswith("/") and not path.startswith("//") and ".." not in path and "?" not in path:
        return "https://image.tmdb.org/t/p/w780" + quote(path, safe="/.")
    return None


def _clip(value: str, length: int) -> str:
    return value if len(value) <= length else value[: length - 1] + "…"


def _html(value: object, limit: int) -> str:
    result = ""
    for char in str(value or ""):
        part = escape(char)
        if len(result) + len(part) > limit - 1:
            return result + "…"
        result += part
    return result


def _asset_size(asset: Mapping[str, Any]) -> int:
    try:
        return max(0, int(asset.get("size_bytes") or asset.get("size") or asset.get("file_size") or 0))
    except (TypeError, ValueError):
        return 0


def _size_text(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.2f}{unit}"
        value /= 1024
    raise AssertionError("unreachable size unit")
