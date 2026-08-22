from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _pointer(parts: list[str]) -> str:
    if not parts:
        return "/"
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in parts)


def compare(expected: Any, actual: Any, ignored: set[str] | None = None) -> list[dict[str, Any]]:
    ignored = ignored or set()
    differences: list[dict[str, Any]] = []

    def visit(left: Any, right: Any, parts: list[str]) -> None:
        pointer = _pointer(parts)
        if pointer in ignored:
            return
        if type(left) is not type(right):
            differences.append(
                {
                    "path": pointer,
                    "kind": "type",
                    "expected": type(left).__name__,
                    "actual": type(right).__name__,
                }
            )
            return
        if isinstance(left, dict):
            for key in sorted(set(left) | set(right)):
                child = [*parts, str(key)]
                if key not in left:
                    differences.append({"path": _pointer(child), "kind": "unexpected", "actual": right[key]})
                elif key not in right:
                    differences.append({"path": _pointer(child), "kind": "missing", "expected": left[key]})
                else:
                    visit(left[key], right[key], child)
            return
        if isinstance(left, list):
            if len(left) != len(right):
                differences.append(
                    {"path": pointer, "kind": "length", "expected": len(left), "actual": len(right)}
                )
            for index, (left_item, right_item) in enumerate(zip(left, right)):
                visit(left_item, right_item, [*parts, str(index)])
            return
        if left != right:
            differences.append({"path": pointer, "kind": "value", "expected": left, "actual": right})

    visit(expected, actual, [])
    return differences


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two JSON/YAML-1.2-compatible contract documents.")
    parser.add_argument("expected", type=Path)
    parser.add_argument("actual", type=Path)
    parser.add_argument("--ignore", action="append", default=[], help="Exact JSON Pointer to ignore")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    expected = json.loads(args.expected.read_text(encoding="utf-8"))
    actual = json.loads(args.actual.read_text(encoding="utf-8"))
    differences = compare(expected, actual, set(args.ignore))
    payload = {"equal": not differences, "difference_count": len(differences), "differences": differences}
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0 if not differences else 1


if __name__ == "__main__":
    raise SystemExit(main())
