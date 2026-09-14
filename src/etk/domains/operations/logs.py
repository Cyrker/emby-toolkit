from __future__ import annotations

import html
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

_TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")
_START_MARKER = re.compile(r"Webhook: 收到入库事件\s'(.+?)'，已分派预检任务。")
_END_MARKER = re.compile(r"后台任务\s'Webhook入库:\s(.+?)'\s结束，最终状态:\s处理完成。")
_INTERFERENCE_MARKER = re.compile(r"(?:Webhook: 收到入库事件|项目|预检.+?检测到|开始检查|开始处理|处理完成)\s'(.+?)'")
_LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})(?:[,.]\d+)?\s+-\s+(.*?)\s+-\s+"
    r"(INFO|DEBUG|WARNING|ERROR|CRITICAL)\s+-\s+(.*)$",
    re.DOTALL,
)


def group_log_lines(lines: list[str] | tuple[str, ...]) -> list[str]:
    """以带时间戳的记录为单位分组，堆栈保持原始顺序和缩进。"""
    groups: list[str] = []
    for chunk in lines:
        for line in chunk.splitlines(keepends=True):
            if _TIMESTAMP_REGEX.match(line) or not groups:
                groups.append(line)
            else:
                groups[-1] += ("" if groups[-1].endswith("\n") else "\n") + line
    return groups


class LogAccessDenied(ValueError):
    """The requested filename is outside the legacy app.log namespace."""


class LogFileMissing(FileNotFoundError):
    """The requested legacy log file does not exist."""


class LogReadFailure(OSError):
    """The requested legacy log file exists but cannot be read."""


class LogStorage(Protocol):
    def list_names(self) -> list[str]: ...

    def read_lines(self, filename: str) -> list[str]: ...


class LocalLogStorage:
    """Read-only local filesystem adapter for the legacy ``app.log*`` files."""

    def __init__(self, log_directory: str | Path) -> None:
        self._root = Path(log_directory).expanduser().resolve()

    @property
    def root(self) -> Path:
        return self._root

    def list_names(self) -> list[str]:
        return list(os.listdir(self._root))

    def read_lines(self, filename: str) -> list[str]:
        safe_name, path = self._resolve_log_file(filename)
        if not path.exists():
            raise LogFileMissing(safe_name)
        try:
            with path.open("rt", encoding="utf-8", errors="ignore") as stream:
                return stream.readlines()
        except OSError as exc:
            raise LogReadFailure(safe_name) from exc

    def _resolve_log_file(self, filename: str) -> tuple[str, Path]:
        raw_name = str(filename or "")
        safe_name = secure_filename(raw_name)
        if not safe_name or not safe_name.startswith("app.log"):
            raise LogAccessDenied(raw_name)

        candidate = (self._root / safe_name).resolve()
        try:
            candidate.relative_to(self._root)
        except ValueError as exc:
            raise LogAccessDenied(raw_name) from exc
        return safe_name, candidate


@dataclass(frozen=True, slots=True)
class LogSearchMatch:
    file: str
    line_num: int
    content: str
    date: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "line_num": self.line_num,
            "content": self.content,
            "date": self.date,
        }


@dataclass(frozen=True, slots=True)
class LogContextBlock:
    file: str
    date: str
    lines: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {"file": self.file, "date": self.date, "lines": list(self.lines)}


class LogService:
    """Legacy-compatible log listing, viewing, searching, and context extraction."""

    def __init__(self, storage: LogStorage) -> None:
        self._storage = storage

    def list_log_files(self) -> list[str]:
        names = [name for name in self._storage.list_names() if name.startswith("app.log")]
        names.sort(key=_list_sort_key)
        return names

    def view_log_file(self, filename: str) -> tuple[str, tuple[str, ...]]:
        lines = group_log_lines(self._storage.read_lines(filename))
        lines.reverse()
        safe_name = secure_filename(filename)
        return safe_name, tuple(lines)

    def search_all_logs(self, query: str) -> list[LogSearchMatch]:
        names = [name for name in self._storage.list_names() if name.startswith("app.log")]
        names.sort(key=_search_sort_key)
        normalized_query = query.lower()
        results: list[LogSearchMatch] = []

        for filename in names:
            try:
                lines = self._storage.read_lines(filename)
            except Exception as exc:
                logger.warning("API: 搜索时无法读取文件 %r: %s", filename, exc)
                continue
            for line_num, line in enumerate(lines, 1):
                if normalized_query not in line.lower():
                    continue
                timestamp = _TIMESTAMP_REGEX.search(line)
                results.append(
                    LogSearchMatch(
                        file=filename,
                        line_num=line_num,
                        content=line.strip(),
                        date=timestamp.group(1) if timestamp else "",
                    )
                )

        results.sort(key=lambda item: item.date, reverse=True)
        return results

    def search_logs_with_context(self, query: str) -> list[LogContextBlock]:
        names = sorted(
            [name for name in self._storage.list_names() if name.startswith("app.log")],
            reverse=True,
        )
        normalized_query = query.lower()
        blocks: list[LogContextBlock] = []

        for filename in names:
            try:
                lines = self._storage.read_lines(filename)
            except Exception as exc:
                logger.warning("API: 读取文件 %r 出错: %s", filename, exc)
                continue

            current_block: list[str] = []
            if filename == "app.log" and lines and lines[0].startswith("统一应用日志："):
                hits = [index for index, line in enumerate(lines) if normalized_query in line.lower()]
                indices = sorted(
                    {position for index in hits for position in range(max(1, index - 3), min(len(lines), index + 4))}
                )
                if indices:
                    selected = tuple(lines[index] for index in indices)
                    timestamp = next(filter(None, map(_TIMESTAMP_REGEX.search, selected)), None)
                    blocks.append(
                        LogContextBlock(
                            file=filename, date=timestamp.group(1) if timestamp else "Unknown Date", lines=selected
                        )
                    )
                continue
            if filename.startswith("app.log.task-"):
                if any(normalized_query in line.lower() for line in lines):
                    timestamp = next(filter(None, map(_TIMESTAMP_REGEX.search, lines)), None)
                    blocks.append(
                        LogContextBlock(
                            file=filename,
                            date=timestamp.group(1) if timestamp else "Unknown Date",
                            lines=tuple(lines),
                        )
                    )
                continue
            active_item_name: str | None = None
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue

                start_match = _START_MARKER.search(stripped)
                end_match = _END_MARKER.search(stripped)
                if active_item_name is None:
                    if start_match:
                        item_name = start_match.group(1)
                        if normalized_query in item_name.lower():
                            active_item_name = item_name
                            current_block = [line]
                    continue

                if end_match and end_match.group(1) == active_item_name:
                    current_block.append(line)
                    timestamp = _TIMESTAMP_REGEX.search(current_block[0])
                    blocks.append(
                        LogContextBlock(
                            file=filename,
                            date=timestamp.group(1) if timestamp else "Unknown Date",
                            lines=tuple(current_block),
                        )
                    )
                    active_item_name = None
                    current_block = []
                    continue

                interference_match = _INTERFERENCE_MARKER.search(stripped)
                if interference_match and interference_match.group(1) != active_item_name:
                    continue

                if start_match and start_match.group(1) == active_item_name:
                    current_block = [line]
                    continue

                current_block.append(line)

        blocks.sort(key=lambda item: item.date, reverse=True)
        return blocks


def render_log_html(blocks: list[LogContextBlock], query: str) -> str:
    css_styles = """
    <style>
        :root {
            --bg-color: #1e1e1e;
            --time-color: #6a9955;
            --arrow-color: #569cd6;
            --text-color: #d4d4d4;
            --debug-color: #808080;
            --info-color: #b5cea8;
            --warn-color: #ce9178;
            --error-color: #f44747;
            --highlight-bg: #264f78;
        }
        body {
            background-color: var(--bg-color);
            color: var(--text-color);
            font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
            font-size: 13px;
            line-height: 1.6;
            margin: 0;
            padding: 15px;
            height: 100vh;
            box-sizing: border-box;
        }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #1e1e1e; }
        ::-webkit-scrollbar-thumb { background: #424242; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #4f4f4f; }
        .log-block { margin-bottom: 25px; border-left: 2px solid #333; padding-left: 10px; }
        .block-header {
            font-size: 12px;
            color: #555;
            margin-bottom: 5px;
            font-style: italic;
            border-bottom: 1px dashed #333;
            padding-bottom: 2px;
            display: inline-block;
        }
        .line { display: grid; grid-template-columns: 142px 68px minmax(0,1fr); gap: 10px;
            align-items: start; padding: 9px 6px; border-bottom: 1px solid #303030; }
        [hidden] { display: none !important; }
        .line:hover { background-color: #2a2d2e; }
        .source { display: block; color: #999; font-size: 11px; overflow-wrap: anywhere; margin-top: 3px; }
        .level { font-size: 11px; font-weight: 700; border: 1px solid currentColor;
            text-align: center; border-radius: 4px; padding: 1px 3px; }
        .content { min-width: 0; }
        details { margin-top: 6px; color: #bbb; font-weight: normal; }
        summary { cursor: pointer; color: #c5a66c; }
        pre { margin: 8px 0; padding: 10px; background: #151515; overflow: auto; font-size: 12px;
            white-space: pre-wrap; overflow-wrap: anywhere; }
        .controls { position: sticky; top: 0; background: #252525; border: 1px solid #444; padding: 10px;
            margin-bottom: 12px; display: flex; gap: 16px; flex-wrap: wrap; z-index: 1; }
        select { background: #303030; color: #ddd; border: 1px solid #666; padding: 3px; }
        @media (max-width: 600px) {
            body { padding: 8px; }
            .line { grid-template-columns: 1fr 68px; gap: 5px; }
            .content { grid-column: 1 / -1; }
        }
        .ts {
            color: var(--time-color);
            min-width: 70px;
            margin-right: 10px;
            opacity: 0.8;
            font-size: 12px;
        }
        .msg { white-space: pre-wrap; overflow-wrap: anywhere; }
        .lvl-DEBUG { color: var(--debug-color); }
        .lvl-INFO { color: var(--info-color); }
        .lvl-WARNING { color: var(--warn-color); }
        .lvl-ERROR { color: var(--error-color); font-weight: bold; }
        .lvl-CRITICAL { color: var(--error-color); font-weight: bold; }
        .lvl-RAW { color: var(--text-color); }
        .keyword { background-color: var(--highlight-bg); color: #fff; border-radius: 2px; }
        .arrow { color: var(--arrow-color); margin-right: 5px; font-weight: bold; }
    </style>
    """
    document = [
        f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        {css_styles}
    </head>
    <body>
    <div class="controls">
        <label>日志级别 <select id="level-filter"><option value="">全部</option><option>ERROR</option>
        <option>WARNING</option><option>INFO</option><option>DEBUG</option></select></label>
        <label><input id="hide-health" type="checkbox" checked>隐藏成功的健康检查 / 状态轮询</label>
        <span>最新记录在前，堆栈点击展开；时间为服务端时间</span>
    </div>
    """
    ]

    for block in blocks:
        header = html.escape(block.file)
        if block.date:
            date_only = block.date.split(" ")[0] if " " in block.date else block.date
            header += (
                f" <span style='margin-left: 12px; color: #888; font-weight: normal;'>{html.escape(date_only)}</span>"
            )
        document.append(f'<div class="log-block"><div class="block-header">{header}</div>')

        for raw_line in block.lines:
            line = raw_line.strip()
            if not line:
                continue
            match = _LOG_PATTERN.match(line)
            level, time_text, source, message = "RAW", "—", "未标注来源", line
            if match:
                time_text = f"{match.group(1)} {match.group(2)}"
                level = match.group(4)
                source, message = match.group(3), match.group(5)
            headline, _, detail = message.partition("\n")
            safe_message = html.escape(headline)
            if query:
                safe_message = re.sub(
                    re.escape(html.escape(query)),
                    lambda item: f'<span class="keyword">{item.group(0)}</span>',
                    safe_message,
                    flags=re.IGNORECASE,
                )
            health = bool(
                level == "INFO"
                and re.search(r'"(?:GET|HEAD) /(?:api/)?(?:health|status)[ ?]', headline)
                and re.search(r'" 200(?:\s|$)', headline)
            )
            document.append(
                f'<div class="line lvl-{level}" data-level="{level}" data-health="{int(health)}">'
                f'<span class="ts">{time_text}</span><span class="level">{level}</span>'
                f'<div class="content"><span class="msg">{safe_message}</span>'
                f'<span class="source">{html.escape(source)}</span>'
            )
            if detail:
                document.append(
                    f"<details><summary>异常堆栈 / 补充详情（{len(detail.splitlines())} 行）</summary>"
                    f"<pre>{html.escape(detail)}</pre></details>"
                )
            document.append("</div></div>")
        document.append("</div>")

    document.append("""<script>
        const level = document.getElementById('level-filter');
        const health = document.getElementById('hide-health');
        function filterLogs() {
            document.querySelectorAll('.line').forEach(row => {
                row.hidden = (health.checked && row.dataset.health === '1') ||
                    (level.value && row.dataset.level !== level.value &&
                        !(level.value === 'ERROR' && row.dataset.level === 'CRITICAL'));
            });
        }
        level.addEventListener('change', filterLogs);
        health.addEventListener('change', filterLogs);
        filterLogs();
    </script></body></html>""")
    return "".join(document)


def _list_sort_key(filename: str) -> float:
    if filename == "app.log":
        return -1
    parts = filename.split(".")
    if len(parts) > 2 and parts[-1] == "gz" and parts[-2].isdigit():
        return int(parts[-2])
    return float("inf")


def _search_sort_key(filename: str) -> float:
    if filename == "app.log":
        return -1
    parts = filename.split(".")
    if len(parts) == 3 and parts[0] == "app" and parts[1] == "log" and parts[2].isdigit():
        return int(parts[2])
    return float("inf")
