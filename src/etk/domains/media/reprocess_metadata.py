from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, MutableMapping
from typing import Any, Protocol

from .metadata_quality import metadata_gaps


class ReprocessMetadataProvider(Protocol):
    def details(self, tmdb_id: str, media_type: str) -> Mapping[str, Any]: ...
    def season_details(self, tmdb_id: str, season_number: int) -> Mapping[str, Any]: ...
    def search(self, name: str, media_type: str) -> Mapping[str, Any]: ...


class ReprocessIdentificationError(ValueError):
    def __init__(self, code: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _name(value: Any) -> str:
    return "".join(c for c in unicodedata.normalize("NFKC", str(value or "")).casefold() if c.isalnum())


def identify_root(item: Mapping[str, Any], provider: ReprocessMetadataProvider) -> dict[str, Any]:
    result = dict(item)
    kind = {"Movie": "movie", "Series": "tv"}.get(str(item.get("Type")))
    if kind is None:
        raise ValueError("只支持电影或剧集根身份。")
    ids = dict(item.get("ProviderIds") or {})
    current = str(ids.get("Tmdb") or ids.get("tmdb") or "").strip()
    if current:
        if not current.isdigit():
            raise ValueError("TMDb ID 无效，请先更正。")
        return result
    name = str(item.get("Name") or "").strip()
    match = re.search(r"[（(](\d{4})[）)]", name)
    year = str(item.get("ProductionYear") or (match.group(1) if match else ""))
    if match:
        name = name[: match.start()].strip()
        if item.get("ProductionYear") and str(item["ProductionYear"]) != match.group(1):
            raise ValueError("片名年份与 Emby 年份不一致，请人工确认作品身份。")
    if not _name(name) or not re.fullmatch(r"\d{4}", year):
        raise ValueError("缺少 TMDb ID，且没有明确片名和年份；请在 Emby 确认作品并填写 ID。")
    response = provider.search(name, kind)
    if int(response.get("total_pages") or 1) > 1:
        raise ReprocessIdentificationError("TMDB_AMBIGUOUS", "TMDb 候选过多，无法唯一识别；请重新处理并选择作品。")
    names = {_name(name)}
    if item.get("OriginalTitle"):
        names.add(_name(item["OriginalTitle"]))
    candidates = {
        str(row["id"]): row
        for row in response.get("results", [])
        if isinstance(row, Mapping)
        and str(row.get("id") or "").isdigit()
        and str(row.get("release_date") or row.get("first_air_date") or "")[:4] == year
        and any(_name(row.get(key)) in names for key in ("title", "original_title", "name", "original_name"))
    }
    if len(candidates) != 1:
        raise ReprocessIdentificationError(
            "TMDB_AMBIGUOUS" if candidates else "TMDB_NO_MATCH",
            "TMDb 没有唯一的片名及年份匹配；请人工确认作品身份，不自动选择搜索第一项。",
        )
    ids["Tmdb"] = next(iter(candidates))
    result["ProviderIds"] = ids
    return result


def number(value: Any, label: str) -> int:
    if isinstance(value, bool) or not str(value).isdigit():
        raise ValueError(f"{label}缺失或无效，不能按文件顺序猜测。")
    return int(value)


def prepare_metadata(
    item: Mapping[str, Any],
    root: Mapping[str, Any],
    provider: ReprocessMetadataProvider,
    cache: MutableMapping[tuple[str, int], Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    target = dict(item)
    kind = str(item.get("Type") or "")
    root_id = str((root.get("ProviderIds") or {}).get("Tmdb") or (root.get("ProviderIds") or {}).get("tmdb") or "")
    if kind in {"Movie", "Series"}:
        data = dict(provider.details(root_id, "movie" if kind == "Movie" else "tv"))
        if str(data.get("id")) != root_id:
            raise ValueError("TMDb 返回的作品身份不匹配。")
        target["ProviderIds"] = dict(root.get("ProviderIds") or {})
    else:
        season = number(item.get("IndexNumber") if kind == "Season" else item.get("ParentIndexNumber"), "季号")
        key = (root_id, season)
        if key not in cache:
            payload = provider.season_details(root_id, season)
            if payload.get("season_number") != season:
                raise ValueError("TMDb 返回的季号不匹配。")
            cache[key] = payload
        data = dict(cache[key])
        target.update(_series_tmdb_id=root_id, _season_number=season, _episode_number=None)
        if kind == "Episode":
            season_cast = (data.get("credits") or {}).get("cast") or []
            episode = number(item.get("IndexNumber"), "集号")
            matches = [e for e in data.get("episodes", []) if e.get("episode_number") == episode]
            if len(matches) != 1:
                raise ValueError(f"TMDb 未找到 S{season:02}E{episode:02}，请核对分季/特别篇编号。")
            data = dict(matches[0])
            if data.get("season_number") != season:
                raise ValueError("TMDb 返回的分集季号不匹配。")
            episode_details = getattr(provider, "episode_details", None)
            if metadata_gaps(kind, data.get("name"), data.get("overview")) and callable(episode_details):
                detail = episode_details(root_id, season, episode)
                if not isinstance(detail, Mapping):
                    raise ValueError("TMDb 返回的分集资料格式无效。")
                if (
                    str(detail.get("id")) != str(data["id"])
                    or detail.get("season_number") != season
                    or detail.get("episode_number") != episode
                ):
                    raise ValueError("TMDb 返回的分集身份不匹配。")
                # 分集详情可能比季列表先更新，只补缺项，不以空值覆盖季列表资料。
                if "分集标题" in metadata_gaps(kind, data.get("name"), data.get("overview")) and detail.get("name"):
                    data["name"] = detail["name"]
                if not str(data.get("overview") or "").strip() and detail.get("overview"):
                    data["overview"] = detail["overview"]
            cast = {str(actor["id"]): dict(actor) for actor in season_cast if actor.get("id")}
            for actor in data.get("guest_stars") or []:
                if actor.get("id"):
                    cast.setdefault(str(actor["id"]), dict(actor))
            data["credits"] = {"cast": list(cast.values()), "crew": data.get("crew") or []}
            target["_episode_number"] = episode
        elif kind != "Season":
            raise ValueError("不支持的媒体类型。")
        if season == 0 and not (data.get("credits") or {}).get("cast"):
            # -1 仅用于本次任务内缓存整剧主演，不与有效季号混用。
            series_key = (root_id, -1)
            if series_key not in cache:
                series_data = provider.details(root_id, "tv")
                if str(series_data.get("id")) != root_id:
                    raise ValueError("TMDb 返回的作品身份不匹配。")
                cache[series_key] = series_data
            series_cast = (cache[series_key].get("credits") or {}).get("cast") or []
            if series_cast:
                data["credits"] = {**(data.get("credits") or {}), "cast": series_cast}
                data["_cast_source"] = "series_fallback"
    if not str(data.get("id") or "").isdigit() or not (data.get("name") or data.get("title")):
        raise ValueError("TMDb 没有提供有效身份和标题，不能将文件名当作刮削成功。")
    return target, data
