from __future__ import annotations

import ctypes
import os
import string
from dataclasses import dataclass
from typing import Protocol

from .models import P115Result


@dataclass(frozen=True, slots=True)
class P115LocalDirectoryListing:
    current_path: str
    entries: tuple[dict[str, object], ...]


class P115LocalDirectoryNotFound(FileNotFoundError):
    pass


class P115LocalDirectoryBrowser(Protocol):
    def browse(self, path: str) -> P115LocalDirectoryListing: ...


class OsP115LocalDirectoryBrowser:
    def browse(self, path: str) -> P115LocalDirectoryListing:
        current = str(path or "").strip()
        if not current:
            if os.name == "nt":
                return P115LocalDirectoryListing("", tuple(_windows_drives()))
            current = "/"

        if not os.path.exists(current) or not os.path.isdir(current):
            raise P115LocalDirectoryNotFound(current)

        entries: list[dict[str, object]] = []
        parent = os.path.dirname(current)
        if parent and parent != current:
            entries.append(
                {
                    "name": ".. [返回上一级]",
                    "path": parent,
                    "is_parent": True,
                }
            )
        for name in sorted(os.listdir(current)):
            child = os.path.join(current, name)
            if os.path.isdir(child) and not name.startswith("."):
                entries.append(
                    {
                        "name": name,
                        "path": child,
                        "is_parent": False,
                    }
                )
        return P115LocalDirectoryListing(current, tuple(entries))


class P115LocalDirectoryService:
    def __init__(self, browser: P115LocalDirectoryBrowser) -> None:
        self._browser = browser

    def browse(self, path: object) -> P115Result:
        try:
            listing = self._browser.browse(str(path or "").strip())
            return P115Result(
                {
                    "code": 200,
                    "data": [dict(entry) for entry in listing.entries],
                    "current_path": listing.current_path,
                }
            )
        except P115LocalDirectoryNotFound:
            return P115Result(
                {"code": 404, "message": "目录不存在或不是文件夹"},
                404,
            )
        except PermissionError:
            return P115Result(
                {"code": 403, "message": "没有权限访问该目录，请检查 Docker 映射或系统权限"},
                403,
            )
        except Exception as exc:
            return P115Result({"code": 500, "message": str(exc)}, 500)


def _windows_drives() -> list[dict[str, object]]:
    windll = ctypes.windll  # type: ignore[attr-defined]
    bitmask = int(windll.kernel32.GetLogicalDrives())
    drives: list[dict[str, object]] = []
    for letter in string.ascii_uppercase:
        if bitmask & 1:
            path = f"{letter}:\\"
            drives.append({"name": path, "path": path, "is_parent": False})
        bitmask >>= 1
    return drives


__all__ = [
    "OsP115LocalDirectoryBrowser",
    "P115LocalDirectoryBrowser",
    "P115LocalDirectoryListing",
    "P115LocalDirectoryNotFound",
    "P115LocalDirectoryService",
]
