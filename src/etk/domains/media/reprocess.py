from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus

from .reprocess_identification import ReprocessIdentificationService, identity_snapshot, tmdb_id
from .reprocess_images import IMAGE_CHECKS, NativeReprocessImagePreparation, image_present
from .reprocess_metadata import ReprocessIdentificationError


class MediaReprocessProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...


class MediaItemLookup(Protocol):
    def item(self, item_id: str) -> Mapping[str, Any] | None: ...
    def descendants(self, item_id: str) -> Sequence[Mapping[str, Any]]: ...


class MediaMetadataReprocess(Protocol):
    def cached_reprocess_metadata(
        self, item: Mapping[str, Any], root: Mapping[str, Any]
    ) -> Mapping[str, Any] | None: ...

    def identify_reprocess_root(self, item: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def prepare_reprocess_metadata(
        self,
        item: Mapping[str, Any],
        root: Mapping[str, Any],
        cache: dict[tuple[str, int], Mapping[str, Any]],
    ) -> Mapping[str, Any]: ...
    def reprocess_item_metadata(self, item: Mapping[str, Any], progress: MediaReprocessProgress) -> TaskOutcome: ...

    def execute(
        self,
        task_key: str,
        progress: MediaReprocessProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome: ...


class MediaInfoReprocess(Protocol):
    def execute(
        self,
        progress: MediaReprocessProgress,
        *,
        operation_key: str,
        force_full_update: bool = False,
        emby_item_ids: Sequence[str] | None = None,
    ) -> TaskOutcome: ...


class NativeMediaItemReprocessService:
    """Reprocess one Emby item through the native MediaInfo and metadata owners."""

    def __init__(
        self,
        catalog: MediaItemLookup,
        metadata: MediaMetadataReprocess,
        mediainfo: MediaInfoReprocess,
        *,
        refresh: Callable[[Mapping[str, Any], str], TaskOutcome] | None = None,
        images: NativeReprocessImagePreparation | None = None,
        identification: ReprocessIdentificationService | None = None,
    ) -> None:
        self._catalog = catalog
        self._metadata = metadata
        self._mediainfo = mediainfo
        self._refresh = refresh
        self._images = images
        self._identification = identification

    def execute(
        self,
        item_id: str,
        progress: MediaReprocessProgress,
        *,
        operation_key: str,
        media_info_only: bool = False,
        restore_media_info: bool = True,
        only_missing: bool = False,
        include_descendants: bool = True,
        tmdb_selection: Mapping[str, str] | None = None,
        source_job_id: str | None = None,
    ) -> TaskOutcome:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            raise ValueError("media reprocess item_id must not be empty")
        progress.checkpoint()
        item = self._catalog.item(normalized_id)
        if item is None:
            return TaskOutcome.failed("Emby 中未找到待重新处理的媒体项。", {"item_id": normalized_id})

        confirmed_root = None
        if tmdb_selection is not None:
            try:
                if self._identification is None or not source_job_id:
                    raise ValueError("选片确认器或来源任务缺失，未执行写入。")
                context = self._identification.validate(normalized_id, source_job_id, tmdb_selection)
                item, confirmed_root = context["item"], context["root"]
            except (ValueError, RuntimeError) as exc:
                return TaskOutcome.failed(str(exc), {
                    "item_id": normalized_id, "error_code": getattr(exc, "code", "IDENTIFICATION_FAILED"),
                })

        mediainfo = (
            self._mediainfo.execute(
                progress,
                operation_key=f"{operation_key}:mediainfo",
                force_full_update=True,
                emby_item_ids=(normalized_id,),
            )
            if restore_media_info
            and (media_info_only or not only_missing and item.get("Type") not in {"Series", "Season"})
            else TaskOutcome.skipped("当前条目无需重新注入媒体流。", {"item_id": normalized_id})
        )
        if media_info_only:
            return _with_details(mediainfo, normalized_id, metadata=None)

        root = item
        item_type = str(item.get("Type") or "").strip()
        if item_type in {"Episode", "Season"}:
            series_id = str(item.get("SeriesId") or "").strip()
            if not series_id:
                return TaskOutcome.failed(
                    "待重新处理的剧集项目缺少 SeriesId。",
                    {"item_id": normalized_id, "mediainfo_status": mediainfo.status.value},
                )
            progress.checkpoint()
            root = self._catalog.item(series_id) or {}

        progress.update(5, "正在确认作品身份和所选季集范围。")
        missing_root_id = not (
            (root.get("ProviderIds") or {}).get("Tmdb") or (root.get("ProviderIds") or {}).get("tmdb")
        )
        try:
            root = confirmed_root or self._metadata.identify_reprocess_root(root)
            if missing_root_id:
                root = {**root, "_newly_identified": True}
                progress.update(5, f"已{'人工选择' if confirmed_root else '按片名和年份确认'} TMDb 身份："
                                f"{(root.get('ProviderIds') or {}).get('Tmdb')}")
        except (ValueError, RuntimeError) as exc:
            progress.update(100, str(exc))
            return TaskOutcome.failed(str(exc), {
                "item_id": normalized_id, "needs_identification": True,
                "error_code": getattr(exc, "code", "IDENTIFICATION_FAILED"),
            })
        metadata = self._process_scope(
            item, root, progress, operation_key, only_missing=only_missing, include_descendants=include_descendants,
            confirmed=confirmed_root is not None,
        )
        return _combine(normalized_id, mediainfo, metadata)

    def _process_scope(
        self,
        selected: Mapping[str, Any],
        root: Mapping[str, Any],
        progress: MediaReprocessProgress,
        operation_key: str,
        *,
        only_missing: bool = False,
        include_descendants: bool = True,
        confirmed: bool = False,
    ) -> TaskOutcome:
        if self._refresh is None:
            return TaskOutcome.failed("Emby 元数据回填器未配置，未执行刮削。")
        targets = [selected]
        if selected.get("Type") in {"Episode", "Season"} and root.get("_newly_identified"):
            targets.insert(0, root)
        # 父剧已识别不代表后续入库的季已刮削；独立补齐所属季，不扩展到兄弟集。
        if selected.get("Type") == "Episode" and selected.get("SeasonId"):
            season = self._catalog.item(str(selected["SeasonId"]))
            if (
                season is not None and season.get("Type") == "Season"
                and str(season.get("SeriesId")) == str(root.get("Id"))
                and season.get("IndexNumber") == selected.get("ParentIndexNumber")
            ):
                targets.insert(len(targets) - 1, season)
        if include_descendants and selected.get("Type") in {"Series", "Season"}:
            targets.extend(self._catalog.descendants(str(selected["Id"])))
        targets = list({str(target.get("Id")): target for target in targets}.values())
        original_targets = (
            {str(target["Id"]): identity_snapshot(target, target) for target in targets} if confirmed else {}
        )
        root_refreshed = False

        def guard_identity(target: Mapping[str, Any]) -> None:
            if not confirmed:
                return
            current_root = self._catalog.item(str(root["Id"])) or {}
            root_unchanged = identity_snapshot(current_root, current_root) == root["_confirmed_initial_snapshot"]
            if not root_unchanged and not (root_refreshed and tmdb_id(current_root) == tmdb_id(root)):
                raise ReprocessIdentificationError(
                    "IDENTITY_CHANGED", "执行前目标身份已变化，未覆盖，请重新确认。", 409,
                )
            if str(target["Id"]) != str(root["Id"]):
                current = self._catalog.item(str(target["Id"])) or {}
                if identity_snapshot(current, current) != original_targets[str(target["Id"])]:
                    raise ReprocessIdentificationError(
                        "IDENTITY_CHANGED", "所选季集身份已变化，未覆盖，请重新确认。", 409,
                    )

        cache: dict[tuple[str, int], Mapping[str, Any]] = {}
        if confirmed:
            try:
                assert self._identification is not None
                self._identification.validate_scope(targets, root, cache)
            except (ValueError, RuntimeError) as exc:
                return TaskOutcome.failed(str(exc), {
                    "error_code": getattr(exc, "code", "IDENTIFICATION_FAILED"),
                    "failures": [{"item_id": str(selected["Id"]), "stage": "身份确认", "message": str(exc)}],
                })
        image_cache: dict[str, Any] = {}
        prepared: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        pending: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        failures: list[dict[str, str]] = []
        incomplete: list[dict[str, Any]] = []
        skipped = 0
        reused = 0
        counts = {kind: {"selected": 0, "verified": 0} for kind in ("Movie", "Series", "Season", "Episode")}

        def failed(target: Mapping[str, Any], stage: str, message: str) -> None:
            failures.append({"item_id": str(target.get("Id")), "stage": stage, "message": message})
            progress.update(90, f"{_label(target)}：{stage}失败：{message}")

        for index, target in enumerate(targets, 1):
            progress.checkpoint()
            if confirmed:
                target = {**target, "_confirmed_root_tmdb_id": str(root["ProviderIds"]["Tmdb"])}
            kind = str(target.get("Type"))
            if kind not in counts:
                failed(target, "范围检查", "不支持的媒体类型")
                continue
            counts[kind]["selected"] += 1
            if kind in {"Season", "Episode"} and str(target.get("SeriesId")) != str(root.get("Id")):
                failed(target, "范围检查", "分集不属于所选剧集")
                continue
            if (
                selected.get("Type") == "Season"
                and kind == "Episode"
                and target.get("ParentIndexNumber") != selected.get("IndexNumber")
            ):
                failed(target, "范围检查", "分集不属于所选季")
                continue
            progress.update(
                5 + int(index * 45 / len(targets)), f"{_label(target)}：正在刮削并保存元数据 ({index}/{len(targets)})"
            )
            try:
                guard_identity(target)
                fill_missing = only_missing or (kind == "Season" and selected.get("Type") == "Episode")
                expected = self._metadata.cached_reprocess_metadata(target, root) if fill_missing else None
                cached = expected is not None
                if cached:
                    reused += 1
                    actual = self._catalog.item(str(target["Id"])) or {}
                    target = {**target, **actual}
                else:
                    expected = self._metadata.prepare_reprocess_metadata(target, root, cache)
                expected = dict(expected or {})
                gaps = expected.pop("_metadata_gaps", [])
                if gaps:
                    incomplete.append({
                        "item_id": str(target["Id"]), "name": str(target.get("Name") or ""), "missing": gaps,
                    })
                if self._images is not None:
                    for check in IMAGE_CHECKS:
                        expected.pop(check, None)
                    expected.update(self._images.prepare(target, root, image_cache))
                if cached:
                    if _metadata_present(actual, expected):
                        skipped += 1
                        progress.update(50, f"{_label(target)}：缓存完整，Emby 元数据已齐全，跳过")
                        continue
                    images_only = _metadata_present(
                        actual, {k: v for k, v in expected.items() if k not in IMAGE_CHECKS}
                    )
                    if images_only:
                        target = {**target, "_images_only": True}
                        expected = {k: v for k, v in expected.items() if k in IMAGE_CHECKS}
                    elif set(actual.get("LockedFields") or []) & {"Name", "Overview"}:
                        raise ValueError("名称或简介字段已明确锁定，请先确认是否解锁该字段。")
                    progress.update(
                        50, f"{_label(target)}：复用缓存，{'仅补缺失图片' if images_only else '补充缺失元数据和图片'}"
                    )
                target = {
                    **target,
                    "_people_name_translations": expected.get("_people_name_translations", {}),
                    "_missing_image_types": [
                        t for k, t in IMAGE_CHECKS.items() if expected.get(k) and not image_present(target, t)
                    ],
                }
                for message in expected.pop("_log_messages", []):
                    progress.update(5 + int(index * 45 / len(targets)), f"{_label(target)}：{message}")
                prepared.append((target, expected))
            except Exception as exc:
                failed(target, "刮削", str(exc))
        # 先保存所选季集，防止父级刷新回调读取到尚未准备好的子级数据。
        for target, expected in prepared:
            progress.checkpoint()
            try:
                guard_identity(target)
                progress.update(
                    55,
                    f"{_label(target)}：正在请求 Emby 刷新元数据；"
                    + (
                        "补充缺失图片：" + ", ".join(target["_missing_image_types"]) + "；不替换已有图片"
                        if target.get("_missing_image_types")
                        else "保留已有图片"
                    ),
                )
                result = self._refresh(target, operation_key)
                if result.status is not TaskOutcomeStatus.SUCCEEDED:
                    failed(target, "回填请求", result.message)
                    continue
                pending.append((target, expected))
                if str(target["Id"]) == str(root["Id"]):
                    root_refreshed = True
            except Exception as exc:
                failed(target, "回填请求", str(exc))
        for target, expected in pending:
            progress.checkpoint()
            matched = False
            read_error = ""
            missing_images: list[str] = []
            readback_attempts = 60 if target.get("_missing_image_types") else 10
            total_attempts = readback_attempts * 2 if target.get("_missing_image_types") else readback_attempts
            for attempt in range(total_attempts):
                progress.checkpoint()
                try:
                    actual = self._catalog.item(str(target["Id"])) or {}
                except (ValueError, RuntimeError) as exc:
                    read_error = str(exc)
                    break
                missing_images = [
                    t for k, t in IMAGE_CHECKS.items() if expected.get(k) and not image_present(actual, t)
                ]
                people = actual.get("People") or []
                values = {
                    **actual,
                    "_tmdb_id": (actual.get("ProviderIds") or {}).get("Tmdb"),
                    "_imdb_id": (actual.get("ProviderIds") or {}).get("Imdb"),
                    "_primary_image": bool((actual.get("ImageTags") or {}).get("Primary")),
                    **{key: image_present(actual, kind) for key, kind in IMAGE_CHECKS.items()},
                    "_actor_count": sum(p.get("Type") == "Actor" for p in people),
                    "_director_count": sum(p.get("Type") == "Director" for p in people),
                    "_actor_images": sum(p.get("Type") == "Actor" and bool(p.get("PrimaryImageTag")) for p in people),
                    "_director_images": sum(
                        p.get("Type") == "Director" and bool(p.get("PrimaryImageTag")) for p in people
                    ),
                }
                if all(
                    _people_names_match(actual, value)
                    if key == "_people_name_translations"
                    else values.get(key, 0) >= value
                    if key in {"_actor_count", "_director_count", "_actor_images", "_director_images"}
                    else set(value).issubset(set(values.get(key) or []))
                    if key == "Genres"
                    else str(values.get(key) or "").strip() == str(value).strip()
                    for key, value in expected.items()
                ):
                    matched = True
                    break
                if attempt == readback_attempts - 1:
                    if not missing_images or total_attempts == readback_attempts:
                        break
                    # 首次入库回调可能先于图片缓存就绪；只补偿一次缺图，不重放文字刮削。
                    progress.update(
                        90, f"{_label(target)}：Emby 仍缺少 {', '.join(missing_images)}，执行一次仅补缺图刷新"
                    )
                    try:
                        result = self._refresh(
                            {**target, "_images_only": True, "_missing_image_types": missing_images},
                            f"{operation_key}:missing-images",
                        )
                        if result.status is not TaskOutcomeStatus.SUCCEEDED:
                            read_error = f"补图请求未确认：{result.message}"
                            break
                    except Exception as exc:
                        read_error = f"补图请求失败：{exc}"
                        break
                if attempt < total_attempts - 1:
                    time.sleep(0.5)
            if matched:
                counts[str(target["Type"])]["verified"] += 1
                progress.update(
                    95, f"{_label(target)}：Emby 回读已确认，标题：{expected.get('Name', target.get('Name', ''))}"
                )
                if "_actor_count" in expected:
                    progress.update(
                        95,
                        f"{_label(target)}：演员 {values['_actor_count']} 人、导演 {values['_director_count']} 人；"
                        f"人物头像 {values['_actor_images'] + values['_director_images']} 张；"
                        f"分集主图 {'已存在' if values['_primary_image'] else '无'}",
                    )
            else:
                failed(
                    target,
                    "回读校验",
                    read_error
                    or (
                        "Emby 仍缺少图片：" + ", ".join(missing_images)
                        if missing_images
                        else "Emby 元数据、演职人员或图片未与刮削结果一致，不能标记成功"
                    ),
                )
        verified = sum(count["verified"] for count in counts.values())
        details = {
            "selected": len(targets),
            "saved": len(prepared),
            "verified": verified,
            "failed": len(failures),
            "skipped": skipped,
            "cache_reused": reused,
            "only_missing": only_missing,
            "include_descendants": include_descendants,
            "by_type": counts,
            "failures": failures,
            "incomplete": incomplete,
        }
        message = (
            f"媒体元数据处理结束：选择 {len(targets)} 项，回填确认 {verified} 项，"
            f"跳过 {skipped} 项，失败 {len(failures)} 项。"
        )
        progress.update(100, message)
        if failures:
            return (
                TaskOutcome.partial(message, details) if verified or skipped else TaskOutcome.failed(message, details)
            )
        if incomplete:
            return TaskOutcome.partial(f"已有元数据已回填，{len(incomplete)} 项中文资料待补齐。", details)
        return TaskOutcome.succeeded(message, details)


class NativeMediaItemReprocessOperation:
    def __init__(
        self,
        service: NativeMediaItemReprocessService,
        *,
        notify: Callable[[str, TaskOutcome, str], Mapping[str, Any]] | None = None,
        schedule_retry: Callable[..., Mapping[str, Any]] | None = None,
        update_retry_notification: Callable[[str, int], Mapping[str, Any]] | None = None,
    ) -> None:
        self._service = service
        self._notify = notify
        self._schedule_retry = schedule_retry
        self._update_retry_notification = update_retry_notification

    def execute(self, parameters: Mapping[str, Any], progress: MediaReprocessProgress) -> Mapping[str, Any]:
        allowed = {
            "item_id", "media_info_only", "restore_media_info", "only_missing", "notify_library",
            "metadata_retry_count",
            "notification_member_key",
            "include_descendants",
            "tmdb_selection", "source_job_id",
        }
        extras = set(parameters) - allowed
        if extras:
            raise ValueError(f"media reprocess operation contains unsupported fields: {sorted(extras)!r}")
        item_id = str(parameters.get("item_id") or "").strip()
        retry_count = parameters.get("metadata_retry_count", 0)
        notification_member_key = str(parameters.get("notification_member_key") or "").strip()
        if type(retry_count) is not int or not 0 <= retry_count <= 3:
            raise ValueError("元数据补抓次数无效")
        for field in ("media_info_only", "restore_media_info", "only_missing", "notify_library", "include_descendants"):
            if field in parameters and not isinstance(parameters[field], bool):
                raise TypeError(f"media reprocess operation field {field!r} must be boolean")
        outcome = self._service.execute(
            item_id,
            progress,
            operation_key=f"media.reprocess-item:{getattr(progress, 'job_id', item_id)}",
            media_info_only=bool(parameters.get("media_info_only", False)),
            restore_media_info=bool(parameters.get("restore_media_info", True)),
            only_missing=bool(parameters.get("only_missing", False)),
            include_descendants=bool(parameters.get("include_descendants", True)),
            **({"tmdb_selection": parameters["tmdb_selection"], "source_job_id": parameters.get("source_job_id")}
               if "tmdb_selection" in parameters else {}),
        )
        details = dict(outcome.details)
        if self._schedule_retry is not None and details.get("metadata", {}).get("incomplete"):
            progress.checkpoint()
            details["metadata_retry"] = dict(self._schedule_retry(
                outcome, f"media.reprocess-item:{getattr(progress, 'job_id', item_id)}", retry_count,
                **({"notification_member_key": notification_member_key, "notification_item_id": item_id}
                   if notification_member_key else {}),
            ))
            progress.update(100, "中文资料待补齐；" + (
                "已安排延迟补抓，不占用当前任务队列" if details["metadata_retry"]["status"] == "scheduled"
                else details["metadata_retry"].get("message", "请手动重新刮削")
            ))
        metadata = details.get("metadata")
        if (
            retry_count > 0
            and notification_member_key
            and self._update_retry_notification is not None
            and outcome.status is TaskOutcomeStatus.SUCCEEDED
            and isinstance(metadata, Mapping)
        ):
            incomplete_ids = {
                str(value.get("item_id") or "")
                for value in metadata.get("incomplete", [])
                if isinstance(value, Mapping)
            }
            failure_ids = {
                str(value.get("item_id") or "")
                for value in metadata.get("failures", [])
                if isinstance(value, Mapping)
            }
            if item_id not in incomplete_ids and item_id not in failure_ids:
                progress.checkpoint()
                try:
                    details["retry_notification"] = dict(
                        self._update_retry_notification(notification_member_key, retry_count)
                    )
                except Exception:
                    details["retry_notification"] = {"status": "unconfirmed"}
                    progress.update(100, "元数据补抓成功，但 Telegram 原通知更新未确认。")
        if parameters.get("notify_library") and self._notify is not None:
            progress.checkpoint()
            try:
                details["notification"] = dict(
                    self._notify(item_id, outcome, f"media.reprocess-item:{getattr(progress, 'job_id', item_id)}")
                )
            except Exception:
                details["notification"] = {"status": "unconfirmed"}
                progress.update(100, "入库结果通知未确认，请检查 Telegram 配置和任务日志；不重复刮削。")
        return {
            "status": outcome.status.value,
            "message": outcome.message,
            "details": details,
        }


def _metadata_present(item: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    people = item.get("People") or []
    values = {
        **item,
        "_tmdb_id": (item.get("ProviderIds") or {}).get("Tmdb"),
        "_imdb_id": (item.get("ProviderIds") or {}).get("Imdb"),
        "_primary_image": bool((item.get("ImageTags") or {}).get("Primary")),
        **{key: image_present(item, kind) for key, kind in IMAGE_CHECKS.items()},
        "_actor_count": sum(p.get("Type") == "Actor" for p in people),
        "_director_count": sum(p.get("Type") == "Director" for p in people),
        "_actor_images": sum(p.get("Type") == "Actor" and bool(p.get("PrimaryImageTag")) for p in people),
        "_director_images": sum(p.get("Type") == "Director" and bool(p.get("PrimaryImageTag")) for p in people),
    }
    return all(
        _people_names_match(item, value)
        if key == "_people_name_translations"
        else values.get(key, 0) >= value
        if key in {"_actor_count", "_director_count", "_actor_images", "_director_images"}
        else bool(values.get(key))
        if key == "Genres"
        else str(values.get(key) or "") == str(value)
        if key == "_tmdb_id"
        else bool(str(values.get(key) or "").strip())
        for key, value in expected.items()
    )


def _people_names_match(item: Mapping[str, Any], translations: Mapping[str, str]) -> bool:
    return not any(
        person.get("Type") in {"Actor", "Director"} and person.get("Name") in translations
        for person in item.get("People") or []
    )


def _combine(item_id: str, mediainfo: TaskOutcome, metadata: TaskOutcome) -> TaskOutcome:
    details = {
        "item_id": item_id,
        "mediainfo_status": mediainfo.status.value,
        "metadata_status": metadata.status.value,
        "mediainfo": dict(mediainfo.details),
        "metadata": dict(metadata.details),
    }
    failures = sum(outcome.status is TaskOutcomeStatus.FAILED for outcome in (mediainfo, metadata))
    if failures == 2:
        return TaskOutcome.failed("媒体信息与元数据重新处理均失败。", details)
    if failures or any(outcome.status is TaskOutcomeStatus.PARTIAL for outcome in (mediainfo, metadata)):
        message = "媒体项重新处理部分完成。"
        if metadata.details.get("incomplete") and not metadata.details.get("failures"):
            message = "已有元数据已回填，中文资料待补齐。"
        return TaskOutcome.partial(message, details)
    if all(outcome.status is TaskOutcomeStatus.SKIPPED for outcome in (mediainfo, metadata)):
        return TaskOutcome.skipped("媒体项无需重新处理。", details)
    return TaskOutcome.succeeded("媒体项重新处理完成。", details)


def _with_details(outcome: TaskOutcome, item_id: str, *, metadata: TaskOutcome | None) -> TaskOutcome:
    details = {
        "item_id": item_id,
        "mediainfo_status": outcome.status.value,
        "metadata_status": metadata.status.value if metadata is not None else "not_requested",
        "mediainfo": dict(outcome.details),
    }
    return TaskOutcome(outcome.status, outcome.message, details)


__all__ = ["NativeMediaItemReprocessOperation", "NativeMediaItemReprocessService"]


def _label(item: Mapping[str, Any]) -> str:
    kind = str(item.get("Type") or "")
    if kind == "Episode":
        return f"分集 S{item.get('ParentIndexNumber')}E{item.get('IndexNumber')} [{item.get('Id')}]"
    if kind == "Season":
        return f"第 {item.get('IndexNumber')} 季 [{item.get('Id')}]"
    return f"{kind} {item.get('Name') or item.get('Id')}"
