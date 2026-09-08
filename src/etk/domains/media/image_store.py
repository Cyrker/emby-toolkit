from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

MAX_IMAGE_BYTES = 32 * 1024 * 1024
CACHE_TOKEN_PREFIX = "etk-cache://"
_MIME_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/avif": ".avif",
    "image/svg+xml": ".svg",
}


class SqlAlchemyMediaImageStore:
    """Own image-cache records and bounded files without legacy database globals."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: Callable[[], Settings],
        data_root: Path,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._data_root = data_root.resolve(strict=False)

    @staticmethod
    def cache_token(content_hash: str) -> str:
        return f"{CACHE_TOKEN_PREFIX}{str(content_hash or '').strip().lower()}"

    def get(self, content_hash: str) -> dict[str, Any] | None:
        normalized = str(content_hash or "").strip().lower()
        if re.fullmatch(r"[a-f0-9]{64}", normalized) is None:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT content_hash, relative_path, mime_type, byte_size
                        FROM media_image_cache
                        WHERE content_hash = :content_hash
                        ORDER BY last_accessed_at DESC NULLS LAST
                        LIMIT 1
                        """
                    ),
                    {"content_hash": normalized},
                )
                .mappings()
                .first()
            )
            if row is not None:
                uow.session.execute(
                    text("UPDATE media_image_cache SET last_accessed_at = NOW() WHERE content_hash = :hash"),
                    {"hash": normalized},
                )
                uow.commit()
        if row is None:
            return None
        path = self._safe_path(str(row.get("relative_path") or ""))
        if path is None or not path.is_file():
            return None
        return {**dict(row), "path": str(path)}

    def get_source(self, source_url: str) -> dict[str, Any] | None:
        source = str(source_url or "").strip()
        if not source:
            return None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT source_url, content_hash, relative_path, mime_type, byte_size
                        FROM media_image_cache
                        WHERE source_url = :source_url
                        """
                    ),
                    {"source_url": source},
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                uow.session.execute(
                    text("UPDATE media_image_cache SET last_accessed_at = NOW() WHERE source_url = :source_url"),
                    {"source_url": source},
                )
                uow.commit()
        if row is None:
            return None
        path = self._safe_path(str(row.get("relative_path") or ""))
        if path is None or not path.is_file():
            return None
        return {**dict(row), "path": str(path)}

    def store(self, source_url: str, content: bytes, content_type: str) -> dict[str, Any]:
        source = str(source_url or "").strip()
        mime_type = str(content_type or "").split(";", 1)[0].strip().lower()
        if not source:
            raise ValueError("图片来源不能为空")
        if not content:
            raise ValueError("图片内容为空")
        if len(content) > MAX_IMAGE_BYTES:
            raise ValueError("图片超过 32MB 限制")
        if not mime_type.startswith("image/"):
            raise ValueError("缓存内容不是图片")
        content_hash = hashlib.sha256(content).hexdigest()
        extension = _MIME_EXTENSIONS.get(mime_type) or mimetypes.guess_extension(mime_type) or ".img"
        relative_path = f"{content_hash[:2]}/{content_hash}{extension}"
        final_path = self._safe_path(relative_path)
        if final_path is None:
            raise ValueError("图片仓库路径无效")
        final_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = final_path.parent / f".{uuid4().hex}.tmp"
        try:
            if not final_path.is_file():
                with temporary.open("xb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, final_path)
            with self._unit_of_work() as uow:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO media_image_cache (
                            source_url, content_hash, relative_path, mime_type, byte_size, last_accessed_at
                        ) VALUES (
                            :source_url, :content_hash, :relative_path, :mime_type, :byte_size, NOW()
                        )
                        ON CONFLICT (source_url) DO UPDATE SET
                            content_hash = EXCLUDED.content_hash,
                            relative_path = EXCLUDED.relative_path,
                            mime_type = EXCLUDED.mime_type,
                            byte_size = EXCLUDED.byte_size,
                            last_accessed_at = NOW()
                        """
                    ),
                    {
                        "source_url": source,
                        "content_hash": content_hash,
                        "relative_path": relative_path,
                        "mime_type": mime_type,
                        "byte_size": len(content),
                    },
                )
                uow.commit()
        finally:
            temporary.unlink(missing_ok=True)
        return {
            "source_url": source,
            "content_hash": content_hash,
            "relative_path": relative_path,
            "mime_type": mime_type,
            "byte_size": len(content),
            "path": str(final_path),
        }

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> int:
        discarded = 0
        for source in dict.fromkeys(str(value).strip() for value in sources if str(value).strip()):
            relative_path: str | None = None
            remove_file = False
            with self._unit_of_work() as uow:
                referenced = uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS(
                            SELECT 1
                            FROM media_image_policy_cache
                            WHERE images_json @> CAST(:needle AS jsonb)
                        )
                        """
                    ),
                    {
                        "needle": json.dumps(
                            [{"source_url": source}],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    },
                )
                if referenced:
                    uow.commit()
                    continue
                row = (
                    uow.session.execute(
                        text(
                            """
                            DELETE FROM media_image_cache
                            WHERE source_url = :source_url
                            RETURNING content_hash, relative_path
                            """
                        ),
                        {"source_url": source},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    content_hash = str(row["content_hash"]).strip().lower()
                    relative_path = str(row["relative_path"])
                    token = self.cache_token(content_hash)
                    still_referenced = uow.session.scalar(
                        text(
                            """
                            SELECT EXISTS(
                                SELECT 1 FROM media_image_cache WHERE content_hash = :content_hash
                                UNION ALL
                                SELECT 1 FROM media_metadata
                                WHERE screenshot_hash = :content_hash
                                   OR poster_path = :token OR backdrop_path = :token
                                   OR logo_path = :token OR thumb_path = :token
                            )
                            """
                        ),
                        {"content_hash": content_hash, "token": token},
                    )
                    remove_file = not bool(still_referenced)
                    discarded += 1
                uow.commit()
            if remove_file and relative_path is not None:
                path = self._safe_path(relative_path)
                if path is not None:
                    path.unlink(missing_ok=True)
        return discarded

    def _root(self) -> Path:
        configured = str(self._settings().media_image_archive_path or "").strip()
        if not configured:
            root = self._data_root / "media_images"
        else:
            expanded = Path(os.path.expandvars(os.path.expanduser(configured)))
            root = expanded if expanded.is_absolute() else self._data_root / expanded
        return root.resolve(strict=False)

    def _safe_path(self, relative_path: str) -> Path | None:
        root = self._root()
        parts = tuple(part for part in str(relative_path or "").replace("\\", "/").split("/") if part)
        if not parts or any(part in {".", ".."} for part in parts):
            return None
        path = root.joinpath(*parts).resolve(strict=False)
        try:
            path.relative_to(root)
        except ValueError:
            return None
        return path


__all__ = ["CACHE_TOKEN_PREFIX", "MAX_IMAGE_BYTES", "SqlAlchemyMediaImageStore"]
