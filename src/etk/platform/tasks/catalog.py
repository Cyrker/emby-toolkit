from __future__ import annotations

LEGACY_TASK_KEY_ORDER = (
    "task-chain-high-freq",
    "task-chain-low-freq",
    "populate-metadata",
    "backfill-media-metadata",
    "enrich-aliases",
    "role-translation",
    "actor-translation",
    "process-watchlist",
    "subscribe-assistant-maintenance",
    "intro-fingerprint-backfill",
    "actor-tracking",
    "custom-collections",
    "auto-subscribe",
    "generate-all-covers",
    "generate-custom-collection-covers",
    "refresh_completed_series",
    "scan-monitor-folders",
    "scan-organize-115",
    "full-sync-strm",
    "backfill-organize-record-tmdb-ids",
    "monitor-115-life-events",
    "repair-p115-fingerprints",
    "restore_mediainfo",
    "hdhive-auto-checkin",
    "restore-nfo-and-images",
    "shared-resource-maintenance",
    "sync-all-user-data",
    "generate_embeddings",
    "sync-115-directory-tree",
    "system-auto-update",
    "add-all-series-to-watchlist",
    "process_all_custom_collections",
    "process-single-custom-collection",
    "scan-cleanup-issues",
    "resubscribe-library",
    "update-daily-theme",
    "manual_subscribe_batch",
    "scan_old_seasons_backfill",
    "refresh-collections",
    "update-resubscribe-cache",
    "merge-duplicate-actors",
    "purge-ghost-actors",
    "execute-auto-tagging-rules",
    "fill-studio-images",
    "check-expired-users",
    "share-all-library",
    "recalculate_library_washing_priorities",
    "manual-correct-organize-records",
    "shared-share-status-sync",
    "play-pool-daily-speedtest",
)

P4_NATIVE_TASK_KEYS = frozenset({"task-chain-high-freq", "task-chain-low-freq"})

P5_NATIVE_TASK_KEYS = frozenset(
    {
        "populate-metadata",
        "backfill-media-metadata",
        "role-translation",
        "actor-translation",
        "restore_mediainfo",
        "restore-nfo-and-images",
    }
)

NATIVE_TASK_KEYS = P4_NATIVE_TASK_KEYS | P5_NATIVE_TASK_KEYS
