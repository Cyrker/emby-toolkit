# Goal Tasks

Source: `full-project-refactoring-execution-plan.md`
Status truth: this file.
Execution log and evidence: `goal-log.md`

Allowed status:

- TODO
- DOING
- VERIFYING
- DONE
- BLOCKED
- HUMAN_PENDING
- SKIPPED_HUMAN

| ID | Source | Task | Status | Auto | Evidence | Notes |
|---|---|---|---|---|---|---|
| T000 | Goal bootstrap | Recover an auditable local refactor worktree without overwriting the damaged original worktree | DONE | yes | `goal-log.md#t000` | linked worktree on `codex/refactor-mainline` |
| P0 | Plan §10 P0 | Establish behavior baseline, manifests, sanitized fixtures, and contract tooling | DONE | yes | `goal-log.md#p0` | local source and disposable PG16/Stub evidence only |
| P1 | Plan §10 P1 | Add plaintext FastAPI host, bootstrap, supervisor, and compatibility host | DONE | yes | `goal-log.md#p1` | plaintext image and isolated runtime verified; no production or old environment |
| P2 | Plan §10 P2 | Add settings, DI, SQLAlchemy, Alembic, PostgreSQL 18, and observability platform | DONE | yes | `goal-log.md#p2` | PG18, catalog, image, Compose, shutdown, and cross-review gates passed |
| P3 | Plan §10 P3 | Migrate System, Identity, and Operations contracts | DONE | yes | `goal-log.md#p3` | 41 native bindings; PG18, image, OpenAPI, permission, and rollback gates passed |
| P4 | Plan §10 P4 | Add Celery, Redis, persistent jobs, Coordinator, and all 50 task keys | DOING | yes | pending | start from the P3 stage commit |
| P5 | Plan §10 P5 | Migrate Media, Metadata, Image, Bridge, and Emby events | TODO | yes | pending | |
| P6 | Plan §10 P6 | Migrate 115, ingestion, monitoring, STRM, media info, and playback | TODO | yes | pending | external writes use Fake/Stub adapters |
| P7 | Plan §10 P7 | Migrate subscriptions, watchlist, actors, washing, and MoviePilot adapter | TODO | yes | pending | |
| P8 | Plan §10 P8 | Migrate collections, covers, users, discovery, and secondary administration | TODO | yes | pending | |
| P9 | Plan §10 P9 | Migrate sharing, credit ledger, proxy, Telegram, and HDHive | TODO | yes | pending | |
| P10 | Plan §10 P10 | Migrate frontend to TypeScript, generated OpenAPI client, Vue Query, Vitest, and E2E | TODO | yes | pending | |
| P11 | Plan §10 P11 | Remove active legacy runtime and pass non-functional gates | TODO | yes | pending | legacy deletion only after manifest ownership reaches 100% |
| P12 | Plan §10 P12 local | Destroy, rebuild, and re-verify an isolated Greenfield Compose environment twice | TODO | yes | pending | no production or real external writes |
| LIVE | Goal completion boundary | Run real external-system canary and stable observation | HUMAN_PENDING | no | `goal-log.md#live` | explicitly outside Local Code Complete |
