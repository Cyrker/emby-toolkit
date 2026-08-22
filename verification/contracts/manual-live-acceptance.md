# Live Acceptance Pending

P0-P12 local verification uses only disposable local databases, anonymous fixtures, and Stub/Fake
adapters. The checks below are deliberately excluded from Local Code Complete and remain
`HUMAN_PENDING` until an authorized test or production-like environment is supplied.

## External systems

| System | Manual acceptance | Evidence to retain |
|---|---|---|
| Emby + ETKMediaInfo Bridge | Verify metadata, image sync, ItemID rebind, playback events, chapter writes, and plugin update against a designated test library. | Sanitized request/response diff, test item types, status codes, and Bridge version. |
| 115 | Verify QR/manual authentication, directory reads, STRM/media-info paths, upload/organize, monitoring, play pool, deep delete, fingerprints, intro/credits, and idempotent retries inside an explicitly disposable test directory. | Test-directory boundary, operation IDs, before/after counts, retry result, and cleanup result. |
| MoviePilot | Verify subscription create/update/delete, quota handling, webhook or MCP behavior, and idempotency with a test account. | Sanitized state transitions, quota deltas, and duplicate-request result. |
| Shared center | Verify resource discovery, transfer/share state, virtual-resource behavior, credit ledger balance, and reconciliation with a test tenant. | Sanitized ledger entries, invariant checks, and reconciliation report. |
| Telegram | Verify authorized-user commands, notification templates, retry behavior, and redaction using a test bot/chat. | Sanitized message IDs, command/result mapping, and retry evidence. |
| HDHive | Verify authorization, subscription handoff, check-in, and error mapping with a test account. | Sanitized status transitions and error envelopes. |
| Reverse proxy | Verify real Emby HTTP, Range/HEAD, WebSocket upgrade, 115 redirect, per-user concurrency, and upstream failure behavior. | Status/header diff, WebSocket close codes, and concurrency results. |

## Operational acceptance

1. Use a fresh PostgreSQL 18 volume, Redis volume, `/config`, media test directory, and distinct
   Compose project name. Never reuse the old environment.
2. Confirm every external webhook/scheduler consumer is disabled elsewhere before enabling the
   candidate environment.
3. Run the full canary matrix for Movie, Series, Season, Episode, STRM, physical media, user roles,
   tasks, subscriptions, sharing, and proxy behavior.
4. Observe at least one complete scheduler/task cycle and reconcile jobs, outbox rows, external
   writes, credit/quota deltas, and error logs.
5. Record all differences against `verification/contracts/`; do not mark an external system
   accepted from health checks or page reachability alone.

No real credential, Cookie, token, user identifier, private URL, or customer data may be added to
these artifacts.
