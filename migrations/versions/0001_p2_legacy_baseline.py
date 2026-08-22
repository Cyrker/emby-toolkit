"""Create the self-contained 39-table ETK compatibility baseline.

Revision ID: 0001_p2_legacy_baseline
Revises: None
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator, Sequence
from pathlib import Path

from alembic import op

revision: str = "0001_p2_legacy_baseline"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

BASELINE_SQL = Path(__file__).with_name("p2_legacy_baseline.sql")
BASELINE_SHA256 = "0cf4c65f703b1f852145c902d25cb5481efaa0d8e3e4e2d4ca2fc37e9d915de3"

TABLES = (
    "actor_subscriptions",
    "app_settings",
    "cleanup_index",
    "collections_info",
    "custom_collections",
    "emby_favorite_items",
    "emby_search_index",
    "emby_search_rebuild_state",
    "emby_users",
    "emby_users_extended",
    "failed_log",
    "invitations",
    "media_image_cache",
    "media_image_policy_cache",
    "media_metadata",
    "p115_filesystem_cache",
    "p115_intro_detection_failures",
    "p115_intro_fingerprint_cache",
    "p115_intro_fingerprint_failures",
    "p115_mediainfo_cache",
    "p115_organize_records",
    "p115_upload_records",
    "person_metadata",
    "processed_log",
    "resubscribe_index",
    "resubscribe_rules",
    "shared_completed_season_share_channels",
    "shared_credit_ledger_local",
    "shared_credit_snapshot",
    "shared_rapid_source_files",
    "shared_rapid_sources",
    "shared_virtual_imports",
    "subscribe_assistant_delete_records",
    "subscribe_assistant_snapshots",
    "subscribe_assistant_state",
    "translation_cache",
    "user_media_data",
    "user_templates",
    "washing_priority_groups",
)

SEQUENCES = (
    "actor_subscriptions_id_seq",
    "cleanup_index_id_seq",
    "custom_collections_id_seq",
    "invitations_id_seq",
    "p115_organize_records_id_seq",
    "resubscribe_rules_id_seq",
    "shared_completed_season_share_channels_id_seq",
    "shared_credit_ledger_local_id_seq",
    "shared_rapid_source_files_id_seq",
    "shared_rapid_sources_id_seq",
    "shared_virtual_imports_id_seq",
    "subscribe_assistant_snapshots_id_seq",
    "user_templates_id_seq",
    "washing_priority_groups_id_seq",
)

_DOLLAR_TAG = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def _verified_sql() -> str:
    payload = BASELINE_SQL.read_bytes()
    actual = hashlib.sha256(payload).hexdigest()
    if actual != BASELINE_SHA256:
        raise RuntimeError(f"P2 baseline SQL checksum mismatch: expected {BASELINE_SHA256}, got {actual}")
    return payload.decode("utf-8-sig")


def _statements(source: str) -> Iterator[str]:
    """Split pg_dump DDL without interpreting semicolons inside SQL literals."""

    buffer: list[str] = []
    index = 0
    quote: str | None = None
    dollar_tag: str | None = None
    block_comment_depth = 0
    while index < len(source):
        char = source[index]
        pair = source[index : index + 2]

        if block_comment_depth:
            if pair == "/*":
                block_comment_depth += 1
                index += 2
            elif pair == "*/":
                block_comment_depth -= 1
                index += 2
            else:
                index += 1
            continue

        if dollar_tag is not None:
            if source.startswith(dollar_tag, index):
                buffer.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
            else:
                buffer.append(char)
                index += 1
            continue

        if quote is not None:
            buffer.append(char)
            if char == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    buffer.append(quote)
                    index += 2
                    continue
                quote = None
            index += 1
            continue

        if pair == "--":
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
            buffer.append("\n")
            continue
        if pair == "/*":
            block_comment_depth = 1
            index += 2
            continue
        if char in {"'", '"'}:
            quote = char
            buffer.append(char)
            index += 1
            continue
        if char == "$":
            match = _DOLLAR_TAG.match(source, index)
            if match:
                dollar_tag = match.group(0)
                buffer.append(dollar_tag)
                index = match.end()
                continue
        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                yield statement
            buffer.clear()
            index += 1
            continue
        buffer.append(char)
        index += 1

    if block_comment_depth or quote is not None or dollar_tag is not None:
        raise RuntimeError("unterminated SQL token in P2 baseline")
    trailing = "".join(buffer).strip()
    if trailing:
        yield trailing


def upgrade() -> None:
    for statement in _statements(_verified_sql()):
        op.execute(statement)


def downgrade() -> None:
    for table in reversed(TABLES):
        op.execute(f'DROP TABLE IF EXISTS public."{table}" CASCADE')
    for sequence in reversed(SEQUENCES):
        op.execute(f'DROP SEQUENCE IF EXISTS public."{sequence}" CASCADE')
