# Goal Log

Source: `full-project-refactoring-execution-plan.md`
Task status truth: `goal-tasks.md`

> Reading note (2026-09-12): this is a chronological evidence ledger. Earlier “current”, “pending”, “next” and “no deployment” statements describe their dated runs, not today's global state or permissions. Use the tracker and the current short/long context to resume; do not erase old failures, reinterpret old passing counts as fresh runs, or execute historical destructive Greenfield steps on the deployed environment. The latest context reconciliation is at [Current context reconciliation](#current-context-reconciliation-20260912).

## 2026-08-23 Run

Request:

- Complete P0-P11 Local Code Complete in strict stage order.
- Complete P12 using only a disposable local Greenfield environment and Stub/Fake external systems.
- Preserve pre-existing changes; do not connect to the old environment, push, release, or deploy.

## T000

Status: DONE

Evidence:

- The supplied workspace had a damaged Git administrative state: `.git/refs` was absent and the original worktree showed nearly every tracked path as deleted.
- Reflogs and the public remote independently identified `main@00786dcd632c08a25016276d2531d650ab9ee00c`.
- Unreachable Git trees preserved three versions of the uncommitted execution plan. The newest blob, `acffa8163ed8d0cf5b00dcdc5a66cba6b26d1b6a`, matches the final Greenfield architecture in the goal text.
- Rebuilt only the proven local `main` and `origin/main` refs, leaving the damaged original worktree and its deletions untouched.
- Created the isolated `emby-toolkit-refactor-mainline` linked worktree on local branch
  `codex/refactor-mainline` and restored the exact newest plan blob there.
- Baseline branch status: only `docs/ai-context/full-project-refactoring-execution-plan.md` is untracked.
- Toolchain: macOS `arm64`; Docker CLI 29.4.1; Compose v5.1.2; system Python 3.9.6; Node v25.8.2; npm 11.11.1; uv 0.11.3.
- Docker daemon was not running at baseline (`~/.docker/run/docker.sock` absent).

Notes:

- The refactor worktree is the implementation authority for this Goal.
- The source execution plan is read-only after tracker creation.

## P0

Status: DONE

Evidence:

- Required repository instructions and baseline documents were read in full before implementation.
- Added deterministic, import-free exporters for Route, Task, Table, Config, Frontend Route, and
  Bridge contracts under `scripts/refactor/`.
- Static source inventory is locked at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 explicit application/Method/Path bindings, 50 task keys, and 39 tables.
- Added a durable `ownership-overrides.json` lifecycle overlay, stable contract IDs, validation
  ownership, fixture assignment or explicit fixture exceptions, and a CI contract gate.
- Config baseline contains 103 `APP_CONFIG` keys and 52 standalone `app_settings` keys with zero
  unresolved setting calls. The two duplicate dynamic keys and effective precedence variance are
  preserved explicitly.
- Static DDL contains 39 tables, 68 index statements, 67 unique index names, and three ALTER
  statements. The duplicate `idx_mm_asset_details_gin` statement is recorded as a variance.
- Sanitized fixtures cover Movie, Series, Season, Episode, PickCode/SHA1 MediaInfo, Bridge errors,
  anonymous subscription/sharing/user/task state, reverse proxy scenarios, and local external
  service Stubs.
- A disposable Linux arm64 legacy reference used a new empty `/config`, a fresh PostgreSQL 16
  anonymous volume, an internal Docker network, no media mount, and no external credentials.
- Legacy runtime reached `GET /api/health` in about three seconds. Fifty container-local samples
  returned 200 with p50 0.204 ms and p95 0.360 ms. Host-loopback capture is retained separately.
- Runtime PostgreSQL 16.14 produced 39 tables, 478 columns, 120 indexes, and 60 constraints
  (39 primary keys, 14 unique, five foreign keys, two checks). Static and runtime table sets match.
- The normalized `pg_dump` restores into another empty PostgreSQL 16 database with exactly 39
  tables. `pg_catalog`, schema-only SQL, runtime URL maps, and classified route differences are
  committed under `verification/contracts/`.
- Both runtime Flask applications were exported. Application-aware comparison has five
  runtime-only rules: API SPA `/` and `/<path:path>`, API `/api/health`, plus each Flask app's
  implicit `/static/<path:filename>`. No normalized static-only binding remains.
- The legacy TaskManager accepted and completed a network-disabled synthetic no-external task;
  final status was progress 100, not running, and its worker stopped cleanly.
- Four empty-database query families were sampled 100 times. Recorded p95 values range from
  0.048 ms to 0.258 ms in the disposable local PG16 reference.
- Frontend dependency install and Vite build passed using the Dockerfile-aligned
  `npm ci --legacy-peer-deps` path. The current setup page and login page rendered in the in-app
  browser, and `/DatabaseStats` redirected to `/setup` while the empty reference reported
  `setup_required`; no form was submitted.
- Root VitePress 1.6.4 build passed from a writable temporary source copy: 137 files,
  5,049,547 bytes, tree SHA-256
  `f38cc600c599f3cc3f189a50c6040f05299b52baf984b5bd987647c62a2969f9`.
- The two legacy embedded unittest modules passed on Python 3.12: 66 tests. Two `__new__`-based
  test fixtures now initialize `ai_translator = None`; production behavior and assertions are
  unchanged.
- Contract tools passed 13 unittests; manifest regeneration/check, fixture/runtime artifact
  secret scans, all-repository static Python compile, and `git diff --check` passed.

Recorded baseline variances and limitations:

- Source has 24 non-redirect frontend page routes while the plan text says 23; all 24 are kept.
- Frontend `requiresAdmin` metadata is declared but not enforced by the current router guard.
- `play-pool-daily-speedtest` uses display-name Help fallback; `contribute-mediainfo` has orphan
  Help text without a registered task.
- Plain frontend `npm ci` fails because the lockfile omits three peer packages; the existing
  Dockerfile-compatible `--legacy-peer-deps` install succeeds.
- Docker Hub metadata access failed during two full image builds. The unchanged full build was not
  attempted a third time; the reference used locally cached official base images and a dependency
  image committed from the already prepared disposable test container.
- The legacy process does not exit within 15 seconds after SIGTERM and ends with exit 137 when the
  Docker stop timeout expires.
- Real external-system timeout behavior and business side effects were not exercised. The manual
  matrix is `verification/contracts/manual-live-acceptance.md` and remains `HUMAN_PENDING`.
- All P0 Docker containers are stopped. No production/old config, database, credential, webhook,
  external write, push, release, or deployment was used.

P6.8 implementation evidence:

- Retired the resident 115 Life Event daemon from active startup/reload ownership. The preserved
  `monitor-115-life-events` task key is now an explicit Worker one-shot home-video STRM, subtitle,
  and MediaInfo reconciliation task; help and ordered task artifacts state that it does not consume
  retired Life Events and does not perform screenshot work.
- Added Alembic revision `0010_p6_upload_monitor`, durable config revision/generation control,
  operation sequence, claim/token/lease, request/readback state, local projection state, and native
  completed-source ownership. API GET remains side-effect free; the Coordinator initializes a
  missing default before Observer startup and owns ingress install/apply/clear.
- Worker/Celery owns operation execution and native deep-delete source cleanup. Provider backend
  and numeric account are pinned at the first checkpoint and remote receipt; request-not-sent is
  retryable, unknown outcomes are readback-only, and remote receipt commits before local cache,
  STRM, ingestion, screenshot submission, or source deletion.
- Upload and remote-delete continuations use the immutable operation/snapshot identity. Older local
  continuations cannot overwrite a newer completed source; deep-delete and local cleanup remove
  ledger rows only by full snapshot CAS. Remote delete readback uses pinned account/backend FID
  lookup, so a remote move out of the original parent cannot be misreported as deletion.
- Local source deletion now atomically renames the descriptor-relative target into a random `0700`
  quarantine, rechecks full fingerprint and root/parent attachment, and only then unlinks. A raced
  replacement is restored without overwrite; when the original name is already occupied, both the
  newer path and quarantined replacement are retained with an explicit conflict.
- Added native package exports, route/config/table ownership, process-role and architecture gates,
  upload HTTP fixture, ordered help currentness, and exact OpenAPI gates. Removed an initially added
  nonessential expression index on legacy `p115_upload_records` after PostgreSQL catalog comparison
  detected a 120-to-121 legacy-index drift; the frozen 39-table catalog remains unchanged.

P6.8 verification evidence:

- Current P6 application/architecture/OpenAPI plus process-role/task-catalog gate passed 491 tests
  with one expected Starlette WSGI deprecation warning.
- Current P6.8 application focus passed 26 tests. Current upload-monitor PostgreSQL 18 focus passed
  14 tests, including backend/account checkpoint rejection, config retry bounds, sequence CAS,
  cleanup CAS, mode fences, migration downgrade/upgrade, and the two local deletion race branches.
- Current combined disposable PostgreSQL 18 P3/P5/P6/platform gate passed 154 tests with one
  expected warning. It verified PostgreSQL 18, Alembic head `0010_p6_upload_monitor`, 57 total
  Greenfield tables, the frozen 39-table catalog, 478 columns, 60 constraints, and 120 indexes.
- Disposable PostgreSQL 18 + Redis 7 + real Celery process verification passed 9 tests after the
  Worker custom-task and cleanup lifecycle changes.
- Current repository-wide default verification passed 1132 tests with 204 explicit opt-in
  PostgreSQL/Redis/Celery skips and one expected warning. Required integration gates were run
  separately and passed; their default skips were not accepted as evidence.
- Generated manifests are clean at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 method bindings, 50 task keys, and 39 legacy tables. OpenAPI is clean at
  96 paths / 107 operations. Scoped Ruff check/format, Pyright with zero diagnostics,
  compileall/py_compile, touched-legacy F821, three fixture secret scans, and `git diff --check`
  passed.
- Two independent final read-only reviews returned `proceed` with no unresolved P0/P1/P2 after
  quarantine, keep-mode, sequence, account/backend, readback, lifecycle, ownership, manifest, and
  OpenAPI findings were closed.
- No real 115, Emby, MoviePilot, shared center, Telegram, HDHive, production database, old
  `/config`, external write, push, release, deployment, credential, media, `web_app.py`, or retired
  worktree was used. Live Acceptance remains `HUMAN_PENDING`.

Primary verification commands:

```text
python3 scripts/refactor/export_manifests.py
python3 scripts/refactor/check_manifests.py
python3 scripts/refactor/scan_fixture_secrets.py verification/fixtures
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/http
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/db
python3 -m unittest discover -s verification/contracts -p 'test_*.py'
python3 -m compileall -q -x 'node_modules|frontend|local_data|pyarmor_runtime_000000' .
npm --prefix frontend ci --no-audit --no-fund --legacy-peer-deps
npm --prefix frontend run build
python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
npm ci --prefer-offline --no-audit --no-fund && npm run docs:build
git diff --check
```

Next:

- Implement P1 plaintext FastAPI host, compatibility Flask App, Bootstrap, and RuntimeSupervisor.
- Do not import `web_app` or PyArmor from the new mainline and do not start background work on import.

## P1

Status: DONE

Evidence:

- Added the Python 3.12 `pyproject.toml`/`uv.lock`, plaintext `main.py`, FastAPI factory,
  compatibility Flask host, runtime supervisor, process CLI, compatibility session/response layers,
  `Dockerfile.refactor`, and plaintext entrypoint. The build backend is pinned to
  `hatchling==1.32.0`; `uv lock --check` resolves 144 packages and `uv sync --frozen` checks
  140 installed packages.
- Import probes confirm that `main.py --help` and the FastAPI factory do not import `web_app`,
  PyArmor, config, processors, proxy, task manager, scheduler, or Telegram runtime and do not
  create runtime threads or data directories.
- The compatibility Flask App materializes during ASGI startup, registers all 22 Blueprints in
  baseline order, mounts as the final WSGI fallback, and preserves 345 API rules / 362 normalized
  bindings when the new FastAPI health route is included. Health, setup status, login/SPA fallback,
  static assets, 405 behavior, and Flask Session serialization pass application tests.
- `RuntimeSupervisor` owns start/reload/stop state. Failed reload or stop remains `FAILED`, retains
  the adapter for cleanup retry, and cannot be overwritten by a new start. Shutdown does not close
  processors, the compatibility registry, or the database pool while a managed worker remains live.
- Replaced API-reachable detached gevent timers with a compatibility dispatcher bounded at eight
  workers and 256 total immediate/delayed tasks. Pending timers are cancellable; shutdown is
  retryable; the 257th task is rejected. Dedicated task, monitor, upload, intro, shared listener,
  vector, Telegram, and TG consumer workers now expose checked stop behavior.
- The compatibility psycopg2 pool now serializes creation/closing, tracks active leases, returns a
  connection to its owning pool, and refuses close on timeout. A concurrent lease/close test proves
  the order `get -> put -> close` and successful retry.
- Proxy gevent patching still occurs before legacy network imports. A real local subprocess test
  starts the Proxy listener, sends `SIGTERM`, verifies listener stop and `RuntimeSupervisor.stop()`,
  and observes exit code 143.
- Full permanent verification passes: 37 pytest checks and 66 embedded legacy unittests. Ruff check,
  Ruff format check, Pyright, 162-file Python 3.12 AST parsing, manifest regeneration/check, three
  secret scans, entrypoint syntax, forbidden-import search, and `git diff --check` all pass.
- Built final local arm64 image `sha256:1488fbde2310077a11d21e227ca783fed663258d4aeb51b70a88eabca303c60f`
  (321,363,740 bytes). Runtime inspection confirms `/app/web_app.py` and PyArmor are absent,
  `/app/static/index.html` exists, and `main.py --help` runs.
- Final image Smoke used an internal Docker network, fresh PostgreSQL 16, empty temporary `/config`,
  no published ports, and no external credentials. It returned health 200, `setup_required`, SPA
  HTML, JavaScript MIME `text/javascript`, and initialized exactly 39 tables. `SIGTERM` produced
  task-worker, Scheduler, and Uvicorn shutdown markers. Containers, network, temporary config, and
  anonymous data were removed afterward.
- Two independent read-only re-audits found and then verified fixes for Proxy signal cleanup,
  detached gevent scheduling, bounded delayed work, monitor retry handles, pool races, and failed
  supervisor state. Their final verdict found no remaining P1 blocker/high finding.

Recorded residuals and limitations:

- The final API container records exit code 143 after Docker `SIGTERM`, while logs prove orderly
  application cleanup; this is signal exit semantics, not a forced kill or missing shutdown.
- Starlette emits one `WSGIMiddleware` deprecation warning. The execution plan explicitly requires
  this temporary P1 compatibility mount; its removal remains part of the later legacy retirement.
- This P1 image intentionally exercises the legacy-compatible PostgreSQL 16 initializer. Target
  PostgreSQL 18, Alembic, SQLAlchemy/psycopg 3, Nginx syntax validation, Compose wiring, image digest
  policy, and observability are P2/P12 work and are not claimed complete here.
- No real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, webhook,
  release, push, deployment, or external write was used.

Primary verification commands:

```text
uv lock --check
uv sync --frozen
uv run pytest verification -q
uv run python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
uv run ruff check main.py src/etk verification/application
uv run ruff format --check main.py src/etk verification/application
uv run pyright main.py src/etk verification/application
python3 scripts/refactor/export_manifests.py
python3 scripts/refactor/check_manifests.py
python3 scripts/refactor/scan_fixture_secrets.py verification/fixtures
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/http
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/db
sh -n docker/entrypoint.refactor.sh
docker build -f Dockerfile.refactor -t etk-p1-mainline:local .
git diff --check
```

Next:

- Implement P2 Settings, AppContainer, SQLAlchemy 2, psycopg 3, Alembic baseline, PostgreSQL 18
  integration, Greenfield Compose platform, and baseline observability.
- Keep all real external-system Live Acceptance pending.

## P2

Status: DONE

Evidence:

- Added contract-backed Pydantic v2 Settings, the legacy INI/database adapter, an explicit lazy
  `AppContainer`, and PostgreSQL-backed dynamic/standalone config storage. Non-empty
  `ETK_DATABASE_URL`, legacy `DB_*`, and `[Database]` INI bootstrap paths now resolve the same
  database URL; an empty ETK URL correctly falls back. Dynamic JSONB updates use one atomic merge
  so stale snapshots cannot erase concurrent or unknown keys. Malformed INI loads defaults but is
  not overwritten by an explicit save.
- Added SQLAlchemy 2 Engine/Session/Repository/UoW, psycopg 3 and independent `psycopg_pool`,
  bounded transaction retry for only SQLSTATE `40001`/`40P01`, and retryable lifecycle shutdown.
  A raw-pool shutdown with an active lease now times out explicitly, keeps the pool retryable, and
  succeeds after the lease is returned; `AppContainer` retains only failed resources for retry.
- Added Alembic revision `0001_p2_legacy_baseline` with a self-contained SQL pack. Migration SQL is
  byte-identical to the P0 reference dump with SHA-256
  `0cf4c65f703b1f852145c902d25cb5481efaa0d8e3e4e2d4ca2fc37e9d915de3`.
- Extended the committed PG16 catalog and comparator with `character_maximum_length`,
  `numeric_precision`, `numeric_scale`, and `datetime_precision` for all 478 columns. Synthetic and
  real PG18 mutation tests prove `character(64) -> character(32)` is rejected and restored.
- Added structured stdlib/structlog logging, Trace ID middleware, Prometheus metrics, and recursive
  sensitive-value filtering. Exception text, URL userinfo/query credentials, quoted values with
  spaces, and Authorization Bearer tokens are covered without removing adjacent safe query data.
- Added the digest-pinned PostgreSQL 18 Greenfield Compose platform, migration gate, internal-network
  option, PG18 data-volume path, migration pack in `Dockerfile.refactor`, and a P2 CI workflow. The
  pinned PostgreSQL digest is a multi-architecture OCI index with amd64 and arm64 manifests.
- Real disposable PostgreSQL 18 integration passed: `17 passed`. It asserted version `180006`,
  Alembic upgrade, downgrade/upgrade behavior, UoW rollback, raw pool lifecycle, dynamic config
  merge, type-width mutation rejection, and a catalog match of 39 tables, 478 columns,
  60 constraints, and 120 indexes.
- Final permanent verification passed: `79 passed, 5 skipped, 1 warning` in the default suite; all
  five skipped tests are the PG18 container tests covered by the separate real run. The two legacy
  embedded unittest modules passed 66 tests. Ruff check, Ruff format check, Pyright, 195-file
  Python 3.12 AST parsing, manifest regeneration/check, three fixture/contract secret scans,
  Compose config, forbidden platform/bootstrap import scan, and `git diff --check` passed.
- `uv lock --check` resolves 155 packages and `uv sync --frozen` checks 151 installed packages.
  Migration/catalog assets, dependency lock, workflow, Compose, and final diff passed two
  independent read-only audits after seven high findings were fixed; both final verdicts found no
  remaining blocker/high in their assigned P2 slices.
- Built final local arm64 image
  `sha256:8a11aaacc7e5526a5c97e7c9fe5958681b5fb4e2bef3d6066ed76428e8c375b2`
  (332,394,957 bytes). It contains Alembic and the migration pack, excludes `web_app.py` and
  PyArmor, handles empty ETK URL fallback, redacts the added credential cases, and runs CLI help.
- Final image Smoke used project `etk-p2-commit-20260823`, an internal network, fresh PG18 and
  `/config` volumes, no published ports, and no real external credentials. Migration exited 0;
  catalog comparison returned `match=true`; health and metrics returned 200; Trace ID propagated;
  auth returned `setup_required`. API SIGTERM exited 143 with `oom=false` after task-worker,
  Scheduler, and Uvicorn shutdown markers. All containers, network, and volumes were removed, and
  a label audit found zero remnants.

Recorded residuals and limitations:

- The planned Starlette `WSGIMiddleware` compatibility mount still emits one deprecation warning;
  P11 removes this temporary legacy dependency. The retained legacy runtime also emits existing
  third-party `iterdir` SyntaxWarnings during image startup.
- P2 intentionally leaves the compatibility `init_db()` path active after Alembic; final schema
  comparison remains exact. Removing legacy startup schema work is deferred to P11 by the plan.
- Real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, webhook,
  release, push, deployment, and external writes were not used and remain Live Acceptance pending.

Primary verification commands:

```text
uv lock --check
uv sync --frozen
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest verification/integration/platform -q
uv run pytest verification -q
uv run python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
uv run ruff check main.py src/etk verification/application verification/unit/platform verification/integration/platform scripts/refactor/compare_schema.py
uv run ruff format --check main.py src/etk verification/application verification/unit/platform verification/integration/platform scripts/refactor/compare_schema.py
uv run pyright main.py src/etk verification/application verification/unit/platform verification/integration/platform scripts/refactor/compare_schema.py
python3 scripts/refactor/check_manifests.py
python3 scripts/refactor/scan_fixture_secrets.py verification/fixtures
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/http
python3 scripts/refactor/scan_fixture_secrets.py verification/contracts/db
docker compose -f docker/compose.refactor.yml config -q
docker build -f Dockerfile.refactor -t etk-p2-mainline:local .
git diff --check
```

Next:

- Implement P3 System, Identity, and Operations routers/use cases from the committed route and
  configuration contracts. Keep the compatibility fallback until each route has a verified new owner.
- Keep all real external-system Live Acceptance pending.

## P3

Status: DONE

Evidence:

- Added 41 native FastAPI method bindings across 36 OpenAPI paths before the final Flask
  compatibility mount: eight Identity, two System Configuration, ten standalone settings, eight
  System Integrations, two runtime queries, four log routes, and seven database read routes.
- Added Flask `SecureCookieSessionInterface` compatible session middleware, lazy secret-key
  creation, one-time recovery tokens, and permission policies that re-check administrator state in
  PostgreSQL instead of trusting `emby_is_admin` from the Cookie. Identity validation preserves
  legacy `415/400/500` HTML body failures and permission-before-body ordering; none of the 15 P3
  body operations exposes FastAPI's automatic 422 response.
- Configuration reads retain masking and derived discovery fields. Writes restore masked values,
  preserve unknown/concurrent keys, reject Emby authorization replacement outside setup or
  reauthorization, and use compensated persistence/runtime reload. The native settings snapshot is
  synchronized from the successfully reloaded supervisor state without a second fallible database
  read. Failed reloads restore persisted, in-memory, and runtime state; incomplete compensation
  leaves the supervisor in `FAILED`.
- Added typed gateways and Fake-backed contracts for AI, proxy, Telegram, GitHub release data, and
  Pro actions. License values and Telegram bot URLs are redacted from logs. Real external checks
  remain pending; the AI translation probe remains a lazy compatibility adapter until P11.
- Added native runtime status/task-catalog and log query routes. Runtime logs are recursively
  redacted; log HTML escapes filenames, lines, and queries; text/plain, text/html, JSON, sorting,
  context extraction, and legacy `secure_filename` behavior are frozen by fixtures.
- Added SQLAlchemy 2 read models for core/library/system/ranking statistics, table listing, review
  pagination, and reason normalization. Real disposable PostgreSQL 18 tests exposed and fixed a
  psycopg 3 ambiguous nullable-bind error with explicit text casts. Empty schema, minimal JSONB,
  Asia/Shanghai timezone, ranking, pagination, hostile search text, and all seven default App
  Factory HTTP routes passed against migrated PG18.
- Frozen `verification/contracts/http/p3-openapi.json`: 36 paths, 41 operations, typed request and
  response schemas. Ownership overlay marks exactly 41 route bindings and 41 decorators as `new`,
  each with owner, fixture, validator, and validation owner; manifest regeneration remains clean.
- The target image now includes the frozen 50-key task catalog. Final arm64 image
  `sha256:fced7ecb01a4491f6d71ca69ef28dc47cceb8b99723dd128a8359a5cef04bb8a`
  is 329,851,386 bytes. Network-disabled image smoke returned health 200, loaded all 50 tasks,
  exposed 36/41 OpenAPI paths/operations, preserved invalid-login 415, kept the data directory
  absent, mounted compatibility last, and excluded `web_app.py` and PyArmor.
- A repeated P1 Proxy SIGTERM failure was traced to stopping `WSGIServer` inside the signal callback
  while `serve_forever()` was still initializing its socket. Shutdown now signals an Event and
  stops outside the callback; 20 consecutive subprocess iterations and the final full suite pass
  with exit 143 and runtime cleanup.
- Final default verification passed `142 passed, 8 skipped, 1 warning`; the eight skips are the
  explicitly gated PG18 suites. The P3 PG18 run passed three tests, including default HTTP wiring.
  The two embedded legacy unittest modules passed 66 tests. Ruff check, Ruff format check, Pyright,
  239-file Python 3.12 AST parsing, manifest/OpenAPI checks, fixture/contract secret scans, Compose
  validation, `git diff --check`, and two read-only P3 audits passed. The only warning is the planned
  Starlette `WSGIMiddleware` deprecation.

Recorded residuals and limitations:

- `GET /api/database/stats/subscription` remains on compatibility because the legacy GET mutates
  `subscription_quota_state` and its SQL/result aliases conflict. Database export/import and
  maintenance actions also remain compatibility-owned until their semantics are migrated and
  approved before P11.
- Runtime status still reads process-local legacy task state; P4 replaces it with the persistent Job
  Store. The packaged manifest is a P3/P4 bridge, not the final executable task registry.
- AI, TMDb proxy, Telegram, GitHub, Pro Worker, Emby/plugin, 115, MoviePilot, shared center, and
  HDHive were tested only through local Fake/Stub contracts. No production database, old `/config`,
  real credential, external write, push, release, deployment, or production environment was used.

Primary verification commands:

```text
uv run pytest -p no:cacheprovider verification -q
ETK_RUN_POSTGRES_INTEGRATION=1 uv run --frozen pytest -p no:cacheprovider verification/integration/p3 -q
uv run python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
uv run ruff check main.py src/etk verification/application verification/unit/platform verification/integration/platform verification/integration/p3 verification/contracts/test_p3_openapi.py scripts/refactor/export_openapi.py scripts/refactor/scan_fixture_secrets.py scripts/refactor/compare_schema.py
uv run ruff format --check main.py src/etk verification/application verification/unit/platform verification/integration/platform verification/integration/p3 verification/contracts/test_p3_openapi.py scripts/refactor/export_openapi.py scripts/refactor/scan_fixture_secrets.py scripts/refactor/compare_schema.py
uv run pyright main.py src/etk verification/application verification/unit/platform verification/integration/platform verification/integration/p3 verification/contracts/test_p3_openapi.py scripts/refactor/export_openapi.py scripts/refactor/compare_schema.py
python3 scripts/refactor/check_manifests.py
uv run python scripts/refactor/export_openapi.py --check
uv run python scripts/refactor/scan_fixture_secrets.py verification/fixtures
uv run python scripts/refactor/scan_fixture_secrets.py verification/contracts/http
docker compose -f docker/compose.refactor.yml config -q
docker build -f Dockerfile.refactor -t etk-p3-mainline:local .
git diff --check
```

Next:

- Implement P4 persistent jobs, Celery/Redis transport, Coordinator, cancellation/retry/recovery,
  and the 50-key executable task mapping. Keep the compatibility fallback until P11.
- Keep all real external-system Live Acceptance pending.

## P4

Status: DONE

Evidence:

- Added a PostgreSQL-backed persistent Job Store, attempt/run/outbox tables, Alembic revision
  `0002_p4_persistent_jobs`, Celery 5/Redis 7 delivery, a leader-elected Coordinator, API task
  projection, cancellation, timeout, retry, lease recovery, task chaining, and the executable
  registry for all 50 frozen task keys. Scheduler startup remains disabled by default.
- Made execution fencing explicit across no-effect, inflight, and committed phases. Direct Job
  timeout, cancellation, and shutdown persist terminal/fence state before the single-concurrency
  Worker attempt process is terminated after `ETK_JOB_TERMINATION_GRACE_SECONDS`; effects that may
  have started enter `indeterminate` reconciliation and are never automatically replayed.
- Added signal-safe sticky shutdown handling and Celery initialization-race requeue behavior.
  Control-plane actions now use `prepare -> execute -> status(request_id)` certainty with
  `confirmed-no-effect`, `unknown`, and `observed-complete` states plus persisted abandoned-effect
  markers. Only failures proven to have no external effect may resume the same Job ID.
- Hardened persistent external-effect boundaries: P115 cache replacement occurs only after every
  authoritative page succeeds; shared-center secrets use owner-bound opaque TTL references and do
  not enter durable payloads; semantic effect identities exclude display and secret fields;
  virtual-washing cancellation releases pre-effect receipts; organize children freeze account and
  target bindings; all-failed library sharing no longer reports success; play-pool rewards use an
  atomic receipt ledger.
- Split the Greenfield runtime into API, Worker, Coordinator, migrate, and Docker control-plane
  roles. Only control-plane receives the Docker socket; token storage is control-plane read/write
  and Worker read-only. Target selection is allowlisted by exact Emby Server ID and excludes the
  ETK service by image/name/Compose service identity without treating unrelated Compose metadata
  paths as service identity. Split-role self-update fails closed before a Docker write.
- Final default verification passed `432 passed, 66 skipped, 1 warning` in 11.98 seconds. The 66
  skips are explicit opt-in PostgreSQL/Celery integration cases covered by the separate real local
  runs. The warning is the planned Starlette `WSGIMiddleware` compatibility deprecation for P11.
- Disposable PostgreSQL 18 verification passed `69 passed, 9 skipped`; the schema reached Alembic
  head `0002_p4_persistent_jobs` with 39 legacy tables plus `idempotency_records`, `job_attempts`,
  `job_runs`, and `outbox_events` (43 total). The nine skips are only the independently gated
  Celery/Redis suite.
- Combined disposable PostgreSQL 18 + Redis + real Celery Worker/Coordinator verification passed
  `58 passed, 1 warning` in 83.25 seconds. It covered duplicate delivery, Redis outage/recovery,
  Worker `kill -9` and lease recovery, cancel/timeout, leader failover, idle/active `SIGTERM`,
  PostgreSQL persistence, and Worker runtime control. CI enables this suite with
  `ETK_RUN_CELERY_REDIS_INTEGRATION=1`.
- The two embedded legacy unittest modules passed 66 tests. Pyright reported 0 errors and 0
  warnings; scoped Ruff check and Ruff format check passed across 144 files; all touched legacy
  Python passed `F821`, `py_compile`, and repository-wide `compileall`. Manifest, P4 OpenAPI,
  fixture/HTTP/database secret scans, Compose config, and `git diff --check` were clean.
- Frozen assets remain internally consistent: 126 legacy Python files, 55 Vue files, 22
  Blueprints, 343 route decorators, 367 method/path bindings, 50 task keys, and 39 legacy tables.
  The ordered task catalog was re-exported structurally from AST source signatures.
- Frontend Node behavior tests passed all 4 cases; `frontend npm ci` and the Vite production build
  passed. Root `npm ci` and the VitePress documentation build passed. The tracked Windows root
  dependency snapshot was restored after macOS validation, and `node_modules` is absent from the
  P4 Git diff.
- Built final local arm64 image
  `sha256:14e71aa7f8871fe46ae5c0a39e9d00ddf817431207e458a00f0bd047b585429a`
  (334,763,134 bytes). It contains the frontend assets and excludes `web_app.py` and PyArmor.
- Final isolated smoke used Compose project `etk-p4-gf-0823a`, fresh PG18/Redis/config volumes, an
  internal network, Fake Emby/auth services, no external credentials, and scheduler disabled. API,
  Worker, Coordinator, and control-plane health passed; the schema was PG18/43 tables/head 0002;
  socket/token role boundaries passed; allowlist and exact Server ID prepare succeeded; Server ID
  mismatch returned confirmed-no-effect; split-role update failed closed; a Redis/Celery
  `sync-115-directory-tree` Job succeeded against empty local configuration. Docker Compose
  `down -v --remove-orphans` left zero project containers, networks, or volumes, and the exact
  temporary smoke directory was deleted afterward.
- The local stage commit title is `refactor(p4): add persistent task runtime`; its SHA is recorded
  by Git history rather than amended back into this same commit.

Recorded residuals and limitations:

- Python cannot safely force-stop a provider call inside its thread. A single-concurrency Worker
  attempt process is the isolation boundary: after the grace period the runtime persists a fence
  and then uses `SIGKILL`. A call interrupted after effect start is deliberately indeterminate and
  requires reconciliation; this design does not claim exactly-once external effects.
- Telegram exposes no provider idempotency key, so provider success between send and local ledger
  completion remains at-least-once. Shared center, 115, Telegram, HDHive, and MoviePilot response
  schemas and eventual-consistency behavior remain Live Acceptance pending.
- Play-pool reward receipts are atomic, but a notification after the transaction may still be
  lost. Control-plane intentionally holds the high-privilege Docker socket in the only role that
  can update; production Docker/Emby acceptance remains pending. Greenfield split-role auto-update
  is disabled, with the complete release workflow deferred to a later stage.
- The frontend application bundle remains about 3.37 MB and Vite warns above 500 kB. Existing npm
  `source-map`/`glob` deprecations remain. Starlette WSGI compatibility is retained until P11.
- No real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, old
  `/config`, credential, webhook, release, push, deployment, or external write was used or accepted.

Primary verification commands:

```text
uv run pytest -p no:cacheprovider verification -q
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest -p no:cacheprovider verification/integration/p3 verification/integration/p4 verification/integration/platform -q
ETK_RUN_POSTGRES_INTEGRATION=1 ETK_RUN_CELERY_REDIS_INTEGRATION=1 uv run pytest -p no:cacheprovider verification/integration/p4 -q
uv run python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
uv run ruff check main.py src/etk verification/application/p4 verification/architecture verification/contracts/test_p4_openapi.py verification/integration/p4 verification/unit/platform/p4 scripts/refactor/export_openapi.py
uv run ruff format --check main.py src/etk verification/application/p4 verification/architecture verification/contracts/test_p4_openapi.py verification/integration/p4 verification/unit/platform/p4 scripts/refactor/export_openapi.py
uv run pyright main.py src/etk verification/application/p4 verification/architecture verification/contracts/test_p4_openapi.py verification/integration/p4 verification/unit/platform/p4 scripts/refactor/export_openapi.py
python3 scripts/refactor/export_manifests.py
python3 scripts/refactor/check_manifests.py
uv run python scripts/refactor/export_openapi.py --check
uv run python scripts/refactor/scan_fixture_secrets.py verification/fixtures
uv run python scripts/refactor/scan_fixture_secrets.py verification/contracts/http
uv run python scripts/refactor/scan_fixture_secrets.py verification/contracts/db
docker compose -f docker/compose.refactor.yml config -q
docker build -f Dockerfile.refactor -t etk-p4-mainline:local .
git diff --check
```

Next:

- Implement P5 Media, Metadata, Image, Bridge, and Emby event ownership from the P4 stage commit.
- Keep all real external-system Live Acceptance pending.

## P5

Status: DONE

Implemented:

- Added native Media repositories, services, routers, compatibility gateways, task routing, and
  durable Media Effect processing for metadata, images, cast editing, media-info/chapter editing,
  ItemID binding, final refresh, Bridge ingress/egress, and Emby events. Compatibility execution is
  explicitly owned and retained only until P11.
- Added Alembic revision `0003_p5_media_effects` with a transactional Media Effect ledger, claim
  fencing, terminal/retry states, request hashes, durable Outbox publication, and PostgreSQL
  constraints/indexes.
- Kept P5/P6 ownership honest: P5 owns the 10 `/api/emby/*` inbound operations and six
  ETKMediaInfo outbound operations; the 14 PickCode/SHA1 `/api/p115/mediainfo/*` operations remain
  legacy-owned with `target_stage=P6`.
- Recorded per-event ownership for all 13 Emby events. `metadata.update` and `image.update` use
  durable P5 envelopes with legacy business delivery; the other 11 events remain explicit P7/P9
  compatibility execution.
- Recorded both actual `backfill-media-metadata` callables: the full-scan default and the targeted
  batch path. All six P5 task keys have native envelopes, explicit legacy execution owners, and P11
  retirement metadata.
- Hardened final refresh, MediaInfo, and Chapters effects against Emby authorization drift. The
  normalized URL/User ID/token-hash revision participates in effect identity, is checked before
  delivery and again before the concrete HTTP request, and never persists the source credentials.

Contract and static evidence:

- Bridge Golden Master schema v2 executes an isolated P4 Flask/AST legacy baseline and the native
  FastAPI/httpx implementations against frozen fixtures: 10 inbound and six outbound contracts.
  All six outbound methods accept the complete `2XX` class and preserve a tested `204` response.
- Generated manifests remain clean at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 method bindings, 50 task keys, and 39 legacy tables. OpenAPI is clean at
  63 paths / 70 operations, including 10 Bridge paths.
- Scoped Ruff check/format, Pyright, legacy touched-file F821, Python compileall, legacy embedded
  unittest (66 tests), fixture secret scan, and `git diff --check` passed. The existing repository-wide
  import-order/type-check baseline outside touched scope was not reformatted or broadened into P5.

Runtime and integration evidence:

- Default full verification: 621 passed, 106 skipped, one expected Starlette WSGI deprecation
  warning. The skipped tests require explicit PostgreSQL/Redis/Celery integration flags and were
  run separately where applicable.
- PostgreSQL 18 P5 integration: 36 passed. Combined P3/P4/P5/platform PostgreSQL suite: 109 passed,
  nine Redis/Celery-only skips. Real local PostgreSQL 18 + Redis + Celery regression: 62 passed.
- The P5 image `etk-p5-mainline:local` was rebuilt as arm64 with digest
  `sha256:ebb3e2f429f20a08a31abb77d08396d9579ca3c70bd2dc8aa9d792780ab38c58`.
  Network-disabled image smoke confirmed 50 task definitions, 63 OpenAPI paths / 70 operations,
  10 Bridge paths, `main.py` present, and `web_app.py`/PyArmor absent.
- Disposable Compose project `etk-p5-gf-0824b` ran PostgreSQL 18.6, Redis 7, API, Worker,
  Coordinator, and control-plane on an internal network with schedules disabled. All six resident
  services were healthy; Worker registered `etk.jobs.execute` and `etk.media.effects.execute`.
- The apparent missing `alembic_version` was a probe error, not schema loss: Alembic intentionally
  stores its ledger in `etk_migrations.alembic_version`. The qualified query returned
  `0003_p5_media_effects`; `public` contained exactly 44 tables (39 compatibility + four P4 +
  `media_effects`). API health and OpenAPI passed from inside the internal network.
- Empty-environment HTTP smoke preserved the native 400 envelopes for invalid metadata, image
  refresh/search, image proxy, and plugin-event requests; an unknown Bridge event returned the
  compatible 200 ignore envelope. Plugin-update returned the expected fail-closed 502 while the
  network was intentionally internal; its 200 binary contract is covered only by the local Stub
  Golden Master and remains Live Acceptance pending.
- `docker compose down -v --remove-orphans` removed all project containers, the internal network,
  four named volumes, and the migrate image's anonymous `/config` volume. Follow-up Docker filters
  returned no residual project objects.

Recorded limitations:

- Starlette WSGI compatibility remains until P11. The Celery image still reports the known root-user
  warning and the compatibility dependency `iterdir` reports Python 3.12 syntax warnings.
- Fake/Stub and isolated Compose evidence is not real Emby, Bridge, 115, MoviePilot, shared-center,
  Telegram, HDHive, or production acceptance. No old `/config`, credential, production database,
  webhook, scheduler write, push, release, or deployment was used.
- The local stage commit title is `refactor(p5): migrate media and bridge`; its SHA is recorded by
  Git history rather than amended back into this same commit.

Primary verification commands:

```text
python3 scripts/refactor/export_manifests.py
python3 scripts/refactor/check_manifests.py
uv run python scripts/refactor/export_openapi.py
uv run python scripts/refactor/export_openapi.py --check
uv run pytest -p no:cacheprovider verification -q
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest -p no:cacheprovider verification/integration/p5 -q
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest -p no:cacheprovider verification/integration/p3 verification/integration/p4 verification/integration/p5 verification/integration/platform -q
ETK_RUN_POSTGRES_INTEGRATION=1 ETK_RUN_CELERY_REDIS_INTEGRATION=1 uv run pytest -p no:cacheprovider verification/integration/p4 verification/integration/p5 -q
uv run ruff check <P5 scoped files>
uv run ruff format --check <P5 scoped files>
uv run pyright <P5 scoped files>
uv run python scripts/refactor/scan_fixture_secrets.py verification/fixtures
docker build -f Dockerfile.refactor -t etk-p5-mainline:local .
docker compose -p etk-p5-gf-0824b -f docker/compose.refactor.yml up -d
docker compose -p etk-p5-gf-0824b -f docker/compose.refactor.yml down -v --remove-orphans
git diff --check
```

Next:

- Implement P6 in its documented sub-slice order, beginning with 115 authentication and read-only
  directory browsing. Keep every external write behind Fake/Stub adapters and keep Live Acceptance
  pending.

## P6

Status: DOING

Completed local sub-slices:

- P6.1: 115 authentication, OpenAPI/Cookie QR flows, manual Cookie, credential status, and
  read-only directory browsing.
- P6.2: account-bound, generation-swapped directory cache and complete paginated synchronization.
- P6.3: sorting/rename/episode policy ownership plus immutable media-recognition, naming, and
  organization Dry Run plans.
- P6.4: whole-Plan organization execution, durable effect ledger, fixed-backend authoritative
  readback, records, and reconciliation-only manual correction.
- P6.5: full/home-video STRM and subtitle projection, local filesystem effects, cache CAS, and
  durable Emby path notifications.
- P6.6: MediaInfo cache/probe/restore, native ISO and bounded ffprobe/preid, PickCode/SHA1 repair,
  STRM enrichment, and six native Bridge operations.
- P6.7: durable Watchdog ingestion, scan and callback/polling ItemID binding, MediaInfo injection,
  item-ready, and two-phase deep delete.
- P6.8: durable upload monitoring, one-shot Life Event replacement, immutable upload/readback
  continuation, and race-safe local cleanup.
- P6.9: direct links, playback-pool and primary-account configuration, durable prepare/reclaim
  operations, temporary artifacts, viewer leases, QR authorization, copy/virtual compatibility,
  and bounded server streaming. P6.10 remains incomplete, so P6 is not DONE and no P6 stage commit
  has been created.

P6.1-P6.2 evidence:

- Added native admin-gated FastAPI transports and local Fake/httpx contracts for OpenAPI token and
  Cookie authorization without echoing secrets or connecting a real 115 account.
- Added Alembic revision `0004_p6_p115_directory_cache`, account/revision-bound settings, complete
  pagination evidence, generation fencing, atomic cache activation, stale-account rejection, and
  persistent task registration for the documented P6 directory-sync task keys.
- Provider failure, repeated/incomplete pages, account drift, generation rollback, and local
  PostgreSQL 18 behavior are covered by application, architecture, and integration verification.

P6.3 implementation evidence:

- Added native sorting, rename, and episode-regex repositories/services while preserving valid
  legacy HTTP envelopes. Malformed mixed-element sorting arrays are an explicit fail-closed
  variance: Native returns JSON 500 before provider or persistence rather than saving invalid rule
  values when the legacy client is unavailable. The variance is frozen in
  `verification/fixtures/http/p6-p115-sorting-rules.json` and recorded in route ownership notes.
- Added immutable `p115_organize_plans` and `p115_organize_plan_actions` documents in Alembic
  revision `0005_p6_organize_plans`, with native-only `POST /api/p115/organize/plans/dry-run` and
  `GET /api/p115/organize/plans/{plan_id}` contracts.
- Explicit `request_intent_id` provides strict replay/conflict behavior; omitted intent creates a
  fresh no-effect plan. Any planning error produces a blocked Plan with no actions.
- Frozen preconditions cover account, sorting/rename/episode/policy revisions, runtime config, each
  MediaInfo snapshot, source identity, and target absence. Commit-time PostgreSQL locks recheck all
  mutable evidence before the Plan and actions are inserted atomically.
- MOVE/RENAME effect identity contains original file ID, parent, name, SHA1, size, stable target
  root CID, and authoritative relative target path. It excludes plan/intent/time, input fingerprint,
  display-only category paths, worker count, and dependency action IDs.
- Multi-level directories are explicit ordered `ENSURE_DIRECTORY` actions. One namespace reserves
  every directory, MOVE-intermediate, and final path; file/file cross-phase conflicts and
  directory/file conflicts block the whole Plan. Existing remote file targets remain frozen as
  `destination_must_be_absent=true` for P6.4 fail-closed preflight.
- RENAME depends on MOVE and describes origin identity separately from the MOVE-output parent.
  No-op rename actions are omitted. Existing-target replacement/delete and washing remain an
  explicit P7 seam and are not inferred by the Plan.
- MediaInfo cache evidence supplies resolution, codec/bit depth, HDR/DoVi, frame rate, audio codec,
  channels, and stream count to the compatibility naming renderer. Compatibility mapping defaults
  enrich label-based sorting dimensions without constructing `SmartOrganizer`.
- Caller hints and copied config are recursively scrubbed using normalized secret-key detection.
  Dry Run request DTOs are manually validated to JSON 400 without FastAPI automatic 422; admin
  denial precedes parsing/provider/persistence for sorting, rename, episode regex, and planning.
- Final independent read-only reviews of HTTP/OpenAPI behavior and Plan/effect semantics reported no
  unresolved P0/P1/P2 findings. Neither review modified files or accessed external systems.

P6.3 verification evidence:

- Generated manifests are clean at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 method bindings, 50 task keys, and 39 legacy tables. Config inventory
  reports 53 standalone keys and zero unresolved setting calls; all four P6.3 setting owners are
  `new`.
- OpenAPI is clean at 77 paths / 87 operations. The Dry Run request has exactly the four documented
  fields; Plan and Action DTOs are frozen; all seven P115 POST operations omit automatic 422;
  qrcode 500 remains JSON and episode-regex 500 declares JSON plus HTML.
- P6 application/architecture/OpenAPI scope: 115 passed with one expected Starlette WSGI
  deprecation warning.
- Disposable PostgreSQL 18 P6/platform integration: 30 passed.
- Repository-wide default verification: 739 passed, 119 explicitly flagged integration skips, and
  one expected Starlette WSGI deprecation warning. Required P6 PostgreSQL tests were run separately
  with the integration flag rather than counted as accepted skips.
- Scoped Ruff check and format, Pyright (zero errors/warnings), legacy touched-file F821,
  compileall/py_compile, three fixture/contract secret scans, OpenAPI check, manifest check, and
  `git diff --check` all passed.

P6.4 implementation evidence:

- Added Alembic revision `0006_p6_organize_execution` with execution, global effect,
  execution-action overlay, reconciliation intent, and append-only effect event tables. Whole-Plan
  provider preflight completes before one PostgreSQL Unit of Work reserves the execution, effects,
  P4 Job/attempt, and Transactional Outbox message.
- One execution pins one account and one `openapi` or `cookie` backend. Mutation never falls back
  after it starts; authoritative readback uses the same backend with complete pagination. Provider
  requests are fenced by effect claim tokens, while the P4 attempt remains the sole orchestration
  owner.
- Effect lifecycle and crash recovery distinguish request-not-sent retryability from post-request
  indeterminacy. Provider receipt and authoritative readback commit separately; terminal global
  outcomes remain immutable across later Plans and reconciliation. Reconciliation is read-only and
  never replays a mutation merely because the effect is absent.
- Added native execute/get/reconcile and records GET/DELETE HTTP contracts. Admin denial precedes
  path/body parsing and persistence; manually validated bodies avoid automatic FastAPI `422`.
  Reservations return `202` plus `x-etk-job-id`, intent replay returns `200`, and records DELETE is
  local-only and idempotent without invoking the 115 provider.
- Migrated `scan-organize-115`, `sync-115-directory-tree`, and
  `manual-correct-organize-records` to the P6 native TaskRouter. Scan accepts only an approved
  `execution_id`; manual correction accepts only an execution and approved reconciliation intent,
  rejects legacy mutating payloads before the service call, and executes zero provider mutations.
  P4/P6 double-terminalization now requires the marker and matching persisted Job/attempt terminal
  state, including the post-terminal progress/heartbeat race windows.
- Frozen legacy catalog visibility still advertises `scan-organize-115` for empty-payload chain and
  manual entry points, while the safe native contract now fails closed without `execution_id`.
  `OrganizeRecordsPage.vue` likewise still submits the retired raw mutating correction payload and
  is rejected. No legacy organizer fallback was restored; visibility/help/frontend workflow is a
  later P10 compatibility decision.

P6.4 verification evidence:

- Focused P4/P6 verification passed: 222 tests. Repository-wide default verification passed:
  803 tests with 130 explicit PostgreSQL/Redis/Celery opt-in skips and one expected Starlette WSGI
  deprecation warning.
- Disposable PostgreSQL 18 P6/platform verification passed: 29 tests. Combined
  P3/P4/P5/P6/platform PostgreSQL 18 verification passed: 133 tests with nine Redis/Celery-only
  skips. P3/P5 migration-head/table-count regression passed: 12 tests.
- Disposable PostgreSQL 18 + Redis 7 + Celery regression passed: 98 tests. Migration revision
  `0006_p6_organize_execution` was upgraded from an empty database, downgraded to `0005`, and
  upgraded again to the single Alembic head.
- Latest scoped Ruff check and format check passed for 53 native/migration/verification files;
  legacy touched-file F821, `py_compile`/`compileall`, and Pyright passed with zero errors or
  warnings. OpenAPI remained clean at 82 paths / 92 operations; manifest, fixture secret scans,
  and `git diff --check` passed.
- All provider behavior used Fake/Stub adapters and disposable local services. Production
  PostgreSQL, real 115/Emby and all other Live Acceptance remain `HUMAN_PENDING`.

P6.5 implementation evidence:

- Added native full and home-video STRM/subtitle synchronization with complete, count-checked 115
  pagination, pinned account/backend/config revisions, exact STRM bytes, bounded subtitle downloads,
  PickCode/SHA1 identity preservation, and full desired-cache projection before local effects.
- Added durable schema-v2 STRM action plans and migration `0007_p6_strm_sync`. Plans bind account,
  scope, backend, roots, managed roots, expected/desired cache revisions, exact local write/delete
  identities, current/recovered Emby notifications, authorization revision, and retry state. Missing
  ownership, plan keys, revisions, roots, or action identities fail closed at both application and
  PostgreSQL boundaries.
- Local writes and cleanup use root-anchored `dir_fd`, `O_DIRECTORY`, and `O_NOFOLLOW` operations.
  Writes are atomic within the opened parent; files and directories are quarantined under the
  opened library root before identity/attachment verification, then deleted or restored. Root
  swaps, detached parents, symlink traversal, replacement races, and postcondition drift abort
  before cache/effect confirmation.
- Cache reconciliation, plan confirmation/supersession, Emby path-change effects, and Outbox events
  share one PostgreSQL transaction. Failed local/finalization attempts remain retryable; successor
  confirmation preserves recovery chains across scope/cache/operation changes.
- Authorization-drift recovery now keeps the latest per-path Modified/Deleted semantics across all
  later effects, handles opposite semantics and A-to-B-to-A authorization cycles, and versions a
  recovery effect with the durable plan revision when a terminal `authorization_changed` intent
  would otherwise be reused. Recovered Deleted paths require unique historical library-root
  provenance and are checked absent both before staging and immediately before database commit.
- Current scan semantics override conflicting recovered semantics. Missing Emby URL or API token
  now fails before remote scan, local projection, cache snapshot, or plan staging, preventing a
  terminal `configuration_incomplete` effect from losing an already-committed notification intent.
- Added the native `POST /api/p115/replace_strm` compatibility contract and confirmable Emby HTTP
  path-change delivery. Exact route ownership, Worker wiring, task ownership, migration structure,
  OpenAPI shape, and later-slice deferrals are guarded by P6 architecture/contract verification.
  P6.6 MediaInfo/ISO/ffprobe/preid/fingerprint work and P6.7-P6.10 remain outside this slice.

P6.5 verification evidence:

- Service and local-adapter focused verification passed: 27 and 18 tests. The complete P6.5
  application/architecture focus passed before final gates; the latest full P6 non-PostgreSQL gate
  passed 275 tests with one expected Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 STRM repository/migration focus passed 17 tests. The complete
  P6/platform PostgreSQL 18 gate passed 58 tests; the independent P3/P5 migration-head/table-count
  regression passed 12 tests with the same expected warning.
- Repository-wide default verification passed 897 tests with 147 explicit
  PostgreSQL/Redis/Celery opt-in skips and one expected warning. Scoped Ruff check/format, Pyright
  with zero errors or warnings, compileall, OpenAPI check, manifest check, fixture secret scan, and
  `git diff --check` all passed on the final tree.
- The final read-only effect/repository reviewer returned `proceed` with no remaining P0/P1/P2
  finding after root provenance, final Deleted recheck, semantic precedence, authorization-cycle,
  migration-key, effect-identity, and Emby configuration fixes.
- All remote/provider behavior remained Fake/Stub and all databases were disposable local
  PostgreSQL 18 instances. No real 115, Emby, production database, external write, deployment,
  release, push, old `/config`, credential, media, or `web_app.py` access occurred. Live Acceptance
  remains `HUMAN_PENDING`.

P6.6 implementation evidence:

- Added Alembic revision `0008_p6_mediainfo_cache`, revisioned formatted/RAW cache ownership, a
  persistent provenance-bound probe worklist, and a database trigger that advances revision and
  `updated_at` for compatibility writers without double-advancing native CAS writes.
- Added canonical MediaInfo formatting, bounded direct-url ffprobe/preid, native ISO/Blu-ray
  probing, account-bound fingerprint repair, persistent STRM per-item enrichment, and native
  `restore_mediainfo` execution. Reclaim refreshes the current cache revision; terminal replay is
  invalidated by cache, formatter-policy, or probe-generation drift; all restore cache decisions
  are fenced against read-before-reserve races.
- Worker shares one credential store, cache repository, fingerprint service, process runner,
  policy provider, probe service, Bridge client, and persistent media-effect executor. The P5 task
  envelope remains stable, but `restore_mediainfo` cannot execute its legacy callable; the P6
  fingerprint task and legacy webhook/helper seams submit persistent-only and fail closed without
  a gateway.
- Six PickCode/SHA1 query, metadata, and metadata-backfill operations are native. Eight intro-sync,
  item-ready, and deep-delete operations remain explicitly legacy/source-only for P6.7/P6.10;
  deep-delete commit declares its real `500` submission-failure response.
- Reviewer findings covering cache revision bypass, worklist recovery, terminal provenance,
  restore races, false 2160p HDR inference, duplicate-SHA1 identity, task-submission TOCTOU,
  deferred contract evidence, and executable legacy ownership were fixed. Both final independent
  read-only re-audits returned `proceed` with no unresolved P0/P1/P2 finding.

P6.6 verification evidence:

- Combined disposable PostgreSQL 18 P3/P5/P6/platform regression passed 113 tests. This includes
  empty-database Alembic head, downgrade/upgrade paths, revision trigger, cache/worklist CAS,
  fingerprint identity, STRM enrichment, P5 durable generated effects, 53-table total, and the
  frozen 39-table legacy catalog.
- Repository-wide default verification passed 1034 tests with 163 explicit PostgreSQL/Redis/Celery
  opt-in skips and one expected Starlette WSGI deprecation warning. Required PostgreSQL tests were
  run separately and are not accepted through those skips.
- Generated manifests are clean at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 method bindings, 50 task keys, and 39 legacy tables. Task ownership is
  14 native / 36 legacy; OpenAPI is clean at 89 paths / 99 operations.
- Scoped Ruff check/format and Pyright passed with zero diagnostics; touched compatibility files
  passed F821; compileall, manifest/OpenAPI currentness, three fixture/contract secret scans, and
  `git diff --check` passed.

P6.7 implementation evidence:

- Added Alembic revision `0009_p6_ingestion_monitor` and a durable ingestion ledger for Watchdog,
  scan, and explicit active-enqueue provenance. Created/moved/deleted events are idempotent,
  coalesced by normalized path, stability sampled, suspended/resumed with monitor configuration,
  and dispatched through the Transactional Outbox. Refresh-only exclusions snapshot and apply the
  configured delay only after stability is established.
- Coordinator recovery now expires stale monitor and binding claims before enabled-state
  transitions; disabled ticks suspend without publishing, failed transitions are retried, and one
  unreadable or concurrently changed file cannot stop the batch. Coordinator and Worker roles both
  install the durable compatibility ingress, while only Coordinator starts Watchdog leadership.
- Physical and STRM ingestion reserve one durable Emby path-notification Media Effect and one
  pending ItemID binding. The binding claim is gated by the live parent monitor lease and confirmed
  notification effect; notification failure terminalizes the binding and monitor atomically.
  Callback and polling paths share PickCode/SHA1 identity rules, preserve historical ItemID replay,
  and reconcile callback claim/confirmation races before any monitor terminal transition.
- MediaInfo injection is a deterministic durable Media Effect keyed by binding ID. Effect and Job
  replay use persisted authorization, MediaInfo, Item/Series/Name snapshots and the existing Job
  idempotency record instead of mutable runtime state. Effect/binding in-progress states and binding
  delivery retries no longer consume the parent monitor terminal budget.
- Native item-ready and two-phase P115 deep-delete contracts are wired through API and Worker roles.
  Deep-delete receipts distinguish request-not-sent retryability from a first unknown shared-cleanup
  outcome, which becomes indeterminate immediately and is never automatically replayed.
- Final independent read-only Worker/state-machine and STRM/enrichment reviews both returned
  `proceed` with no unresolved P0/P1/P2 finding after the callback TOCTOU, notification claim,
  retry-budget, active-provenance, and effect/Job crash-replay fixes.

P6.7 verification evidence:

- Focused ingestion application/HTTP verification passed 24 tests. Full P6
  application/architecture/OpenAPI verification passed 421 tests with one expected Starlette WSGI
  deprecation warning.
- Focused ingestion/deep-delete PostgreSQL 18 verification passed 29 tests. Combined disposable
  PostgreSQL 18 P3/P5/P6/platform verification passed 140 tests, including empty-database Alembic
  head, 55 Greenfield tables, the frozen 39-table legacy catalog, notification uniqueness, binding
  claim fencing, deep-delete receipts, and downgrade/upgrade currentness.
- Disposable PostgreSQL 18 + Redis 7 + real Celery process regression passed 9 tests after Worker
  ingress wiring changed.
- Repository-wide default verification passed 1097 tests with 190 explicit PostgreSQL/Redis/Celery
  opt-in skips and one expected warning. Required integration gates were run separately and passed;
  their default skips were not accepted as evidence.
- Generated manifests are clean at 126 legacy Python files, 55 Vue files, 22 Blueprints,
  343 route decorators, 367 method bindings, 50 task keys, and 39 legacy tables. Scoped Ruff
  check/format, touched-legacy F821, Pyright with zero diagnostics, compileall/py_compile,
  manifest/OpenAPI currentness, three fixture/contract secret scans, `git diff --check`, and final
  status audit passed.
- All external behavior used Fake/Stub adapters and disposable local PostgreSQL/Redis/Celery.
  Real 115, Emby, MoviePilot, shared center, Telegram, HDHive, production database, old `/config`,
  external writes, push, release, deployment, and `web_app.py` access remain outside this run. Live
  Acceptance remains `HUMAN_PENDING`.

P6.9 implementation evidence:

- Added Alembic revision `0011_p6_playback` with exactly four native tables for deterministic
  operations, generationed artifacts, viewer leases, and owner-scoped QR sessions. Canonical
  request hashes, deterministic UUIDs, claim-token/lease-expiry fences, provider checkpoints,
  authoritative readback, Outbox recovery, artifact identity, and terminal conflict rules are
  enforced in PostgreSQL-backed repositories.
- Prepare, reclaim, known-rejection, and local-projection recovery distinguish request-not-sent,
  checkpointed, remotely confirmed, rejected, and indeterminate outcomes. Stable provider identity
  cannot be cleared or drifted; a reclaimed artifact cannot skip an in-flight or indeterminate next
  generation; one request can create at most one new prepare generation.
- ACTIVE leases fence reclamation. RELEASED/EXPIRED leases can only reactivate or rebind to an
  available higher generation with the same artifact key and viewer identity. Copy-play
  `force_new` is a CAS against the PickCode that actually failed, so a delayed stale g1 failure
  reuses a concurrently rebuilt g2 instead of deleting it or creating g3.
- Coordinator ticks expire stale claims, leases, dead dispatches, and QR TTLs. Successful/error QR
  terminal transitions scrub verifier/sign material immediately; expired successful credentials are
  batch-destroyed even if the user never polls again.
- Nineteen native playback Method/Path bindings preserve admin authorization, malformed-body,
  OpenAPI, direct-link, pool, copy, virtual, cleanup, and QR contracts. Server streaming requests
  identity encoding, preserves Range, removes incompatible encoded lengths, and closes response and
  client resources on setup, iteration, and terminal paths.
- Native API, Worker, and proxy roles install the durable compatibility gateway. Legacy active
  clone/source projections stop mutating when the gateway is installed. Signed proxy URLs and P9
  shared-center, ledger, Telegram, and full proxy transport remain outside the P6 durable core.
- `p115_play_pool_config` is classified as secret because it contains nested Cookie/OpenAPI tokens.
  Manifest tooling still counts `web_app.py` as one opaque legacy inventory entry without reading or
  parsing the prohibited source.

P6.9 verification evidence:

- Current P6 application/architecture/unit/process verification passed 596 tests with one expected
  Starlette WSGI deprecation warning. Focused playback PostgreSQL 18 verification passed 18 tests.
- Combined disposable PostgreSQL 18 P3/P5/P6/platform verification passed 172 tests with one expected
  warning. Disposable PostgreSQL 18 + Redis 7 + real Celery verification passed 9 tests.
- Repository-wide default verification passed 1219 tests with 222 explicit PostgreSQL/Redis/Celery
  opt-in skips and one expected warning. Required integration gates were run separately and passed;
  their default skips were not accepted as evidence.
- Generated manifests are clean at 126 opaque-aware legacy Python files, 55 Vue files, 22
  Blueprints, 343 route decorators, 367 method bindings, 50 task keys, and 39 frozen legacy tables.
  OpenAPI is clean at 108 paths / 126 operations, including the exact 19 playback bindings.
- Scoped Ruff check/format, touched-legacy F821, project-environment Pyright with zero diagnostics,
  compileall plus 167-file modified-Python `py_compile`, three fixture/contract secret scans,
  manifest/OpenAPI currentness, and `git diff --check` passed.
- Two independent final read-only reviews returned `proceed` with no unresolved P0/P1/P2 after
  checkpoint, generation, claim, artifact, lease, QR, Outbox, streaming, secret classification, and
  delayed `force_new` CAS findings were closed.
- All external behavior remained Fake/Stub or disposable local infrastructure. No real 115, Emby,
  MoviePilot, shared center, Telegram, HDHive, production database, old `/config`, credential,
  external write, push, release, deployment, `web_app.py`, or retired worktree was used. Live
  Acceptance remains `HUMAN_PENDING`.

P6.10 implementation evidence:

- Added Alembic revision `0012_p6_intro_detection` and a season-scoped durable intro/credits
  operation with deterministic reservation, claim token, lease, revision, cancellation, bounded
  retry/continuation, active-season uniqueness, Transactional Outbox dispatch, and atomic chapter
  cache/Media Effect staging. The legacy fingerprint cache/failure tables retain their frozen
  semantics while normalized credits coverage repairs legacy raw 600-second rows without allowing
  a 180-second downgrade.
- Native extraction processes are timeout/cancellation/output bounded and process-group reaped.
  Signed URLs remain operation-local, account-fenced before and after every use, keyed by account
  and PickCode, cleared on success/error/cancellation, and excluded from durable state and repr.
  Each kind processes at most three episodes per batch; durable rotation reaches later episodes,
  and two complete no-progress sweeps terminalize deterministic extraction/no-match failures.
- Cancellation is part of the transactional claim fence, so late staging/retry/result work cannot
  write cache, effects, or progress after an accepted cancel. Only a true lost claim can bypass the
  bounded state-error attempt path. A final review finding that intro common-match timeout could
  retry forever was fixed by routing it through durable `no_progress_batches`, rotation, and the
  two-sweep terminal budget while preserving large-season coverage.
- Added two auth-none intro-sync POST routes for PickCode and SHA1. They preserve the frozen
  no-body/optional-query/404-or-200 contract, feature-check order, legacy chapter merge rules, and
  three-read revision CAS. Inbound sync updates MediaInfo only, creates no media/outbox effect, and
  keeps P9 snapshot export as a best-effort compatibility seam.
- Added narrow P7 trigger and P9 notification compatibility ports, native Worker/Coordinator/Celery
  execution, native backfill task ownership, route/task/table/Bridge manifests, and OpenAPI
  ownership. API startup installs a `LazyPersistentIntroDetectionGateway`, keeping PostgreSQL lazy;
  Worker keeps the concrete native gateway.

P6.10 verification evidence:

- Final P6 application/architecture/OpenAPI focus passed 579 tests with one expected Starlette WSGI
  deprecation warning. Repository-wide default verification passed 1273 tests with 234 explicit
  PostgreSQL/Redis/Celery opt-in skips and the same warning.
- Combined disposable PostgreSQL 18 P3/P5/P6/platform verification passed 184 tests. This includes
  the intro operation migration/repository, cancellation staging fence, credits normalization and
  self-heal, intro-sync CAS/cache behavior, inbound zero media/outbox effects, empty-database
  Alembic head, 62-table Greenfield catalog, and frozen 39-table legacy catalog. The unchanged
  disposable PostgreSQL 18 + Redis 7 + real Celery process gate remains passed at 9 tests.
- Generated manifests are current at 126 opaque-aware legacy Python files, 55 Vue files, 22
  Blueprints, 343 route decorators, 367 method bindings, 50 task keys, and 39 frozen legacy tables;
  static manifest verification passed 8 tests. OpenAPI is clean at 110 paths / 128 operations.
- Scoped Ruff check/format, configured and P6.10-specific Pyright with zero diagnostics,
  touched-legacy F821, native compileall, touched-legacy py_compile, three fixture/contract secret
  scans, `git diff --check`, and API lazy-start regression checks passed.
- Two independent final read-only reviews returned `proceed` with no remaining P0/P1/P2 after the
  cancellation, deterministic-state retry, batch rotation, signed-URL fence, credits coverage,
  HTTP/CAS/ownership, lazy-start, and common-match timeout findings were closed.
- All external behavior remained Fake/Stub or disposable local infrastructure. No real 115, Emby,
  MoviePilot, shared center, Telegram, HDHive, production database, old `/config`, credential,
  external write, push, release, deployment, `web_app.py`, or retired worktree was used. Live
  Acceptance remains `HUMAN_PENDING`.

P6 stage audit:

- Created local commit `514f508e refactor(p6): migrate p115 and ingestion` on
  `codex/refactor-mainline`; the post-commit worktree was clean before P7 tracking began.

## P7

Status: DONE

Stage / Batch: P7 / P7-FIX

HEAD / Commit:

- Pre-batch HEAD: `f5e20b29` (`13c6d32e` plus the P7-P12 Mac handoff).
- P7-FIX checkpoint: the commit containing this entry; no amend, push, deploy, or release.

Implemented:

- `13c6d32e` already contains subscription runtime, Actor/Watchlist native routes,
  Cleanup/Resubscribe foundations, PostgreSQL operation/quota/runtime state, MoviePilot provider
  execution, process wiring, migration `0013_p7_subscription_runtime`, and P7 verification assets.
- Fixed cancellation/readback recovery so a request checkpoint can no longer become
  `CANCELLED/RELEASED` merely because one authoritative readback is stale. Later observation becomes
  `CONFIRMED/COMMITTED`; exhausted recovery becomes `INDETERMINATE/COMMITTED`; no checkpointed write
  is resent.
- Hardened the PostgreSQL Repository against `mark_cancelled_before_write()` after
  `provider_request_started_at`, and added disposable PostgreSQL 18 version enforcement.
- Added the five required regression classes: successful write plus cancellation and stale read,
  timeout unknown plus cancellation and stale read, later confirmation, final indeterminate, and
  cancellation before provider write.
- Made the Actor HTTP UTF-8 fixture read explicit and persisted the P7-P12 compressed execution
  cadence in the execution plan.

Targeted gate:

- `PYTHONUTF8=0 LC_ALL=C ... test_actor_subscription_http.py -q` -> `19 passed`.
- `pytest verification/application/p7 verification/architecture/p7 -q` -> `158 passed`, with one
  existing Starlette WSGI deprecation warning.
- Digest-pinned disposable PostgreSQL 18, with `ETK_TEST_DATABASE_URL` unset:
  `pytest verification/integration/p7 -q` -> `33 passed`; the session fixture rejects non-18 servers.
- Scoped Ruff -> passed; Ruff format check -> 6 files already formatted; scoped Pyright ->
  `0 errors, 0 warnings`; `git diff --check` -> passed.
- Detailed output: `/tmp/etk-refactor-artifacts/p7-fix-targeted.log` and
  `/tmp/etk-refactor-artifacts/p7-fix-final-targeted.log`.

Stage full gate:

- Not run by batch cadence. Manifest/OpenAPI regeneration and checks, full Ruff/Pyright/pytest,
  cross-stage PostgreSQL 18, Redis/Celery, compileall, secret scans, focused transaction review, and
  P7 integration review remain pending until P7 stage close.

Pending:

- `P7-HTTP/STATE`: Resubscribe, Cleanup, core subscription routes, MoviePilot config, batch delete,
  wiring/contracts, and targeted PostgreSQL 18 delete/quota/Outbox/claim invariants.
- `P7-RUNTIME`: Subscribe Assistant persistent Job/State, remaining native task handlers, retry,
  cancellation, timeout, Worker restart recovery, and Redis/Celery process paths.

Blocker: none for P7-FIX.

Next: execute `P7-HTTP/STATE` without re-exploring the already accepted Actor, Watchlist, or operation
runtime unless a new deterministic failure appears.

Safety boundary:

- No real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, old `/config`,
  external write, push, deployment, release, or production environment was used.

### P7-HTTP/STATE

Stage / Batch: P7 / P7-HTTP/STATE

HEAD / Commit:

- Pre-batch HEAD: `b612384f` (`P7-FIX`).
- P7-HTTP/STATE checkpoint: the commit containing this entry; no amend, push, deploy, or release.

Implemented:

- Added durable Cleanup execution state with frozen snapshots, Job/Attempt/Outbox staging, claims,
  leases, effect checkpoints, idempotent replay, and `FOR UPDATE` conflict protection for execute,
  ignore, delete, and version deletion.
- Completed Resubscribe HTTP/state flow: delete-side quota reservation, atomic delete confirmation and
  create continuation, generation-safe idempotency, complete MoviePilot rule payloads, and rejection
  of mixed unsupported batches without partial state.
- Added native MoviePilot configuration and Strategy contracts with atomic settings updates, injected
  availability probes, compatibility-preserving error behavior, and no real external access.
- Added native core subscription/status/all/batch-delete routes, projection and identity enrichment,
  season provider identity handling, generation-scoped provider DELETE staging, precise index cleanup,
  and local desired-state commits that cannot be promoted back to `WANTED`.
- Wired the new routers, repositories, adapters, migration `0014_p7_cleanup_execution`, fixtures, and
  application/architecture/PostgreSQL 18 verification assets.

Targeted gate:

- `pytest verification/application/p7 verification/architecture/p7 verification/application/p1/test_app_factory_contract.py -q`
  -> `187 passed`, with one existing Starlette WSGI deprecation warning.
- Digest-pinned disposable PostgreSQL 18, with `ETK_TEST_DATABASE_URL` unset:
  `pytest verification/integration/p7 -q` -> `57 passed`; this includes delete, quota, Outbox, claim,
  rollback, concurrency, continuation, and generation invariants.
- Scoped Ruff over 30 changed/untracked Python files -> passed; Ruff format check -> 30 files already
  formatted; scoped Pyright -> `0 errors, 0 warnings`; `git diff --check` -> passed.
- Detailed output: `/tmp/etk-refactor-artifacts/p7-http-state-application-architecture-final.log`,
  `/tmp/etk-refactor-artifacts/p7-http-state-postgres18-final.log`, and
  `/tmp/etk-refactor-artifacts/p7-http-state-static-final.log`.

Stage full gate:

- Not run by batch cadence. Manifest/OpenAPI regeneration and checks, full Ruff/Pyright/pytest,
  cross-stage PostgreSQL 18, Redis/Celery, compileall, secret scans, focused transaction review, and
  P7 integration review remain pending until P7 stage close.

Pending:

- `P7-RUNTIME`: Cleanup native execution, Subscribe Assistant persistent Job/State, remaining native
  task handlers, retry/cancellation/timeout/Worker-restart recovery, and Redis/Celery process paths.

Blocker: none for P7-HTTP/STATE.

Next: execute `P7-RUNTIME`; do not re-explore accepted Actor, Watchlist, or operation runtime unless a
new deterministic test or review finding requires it.

Safety boundary:

- No real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, old `/config`,
  external write, push, deployment, release, or production environment was used.

### P7-RUNTIME / Stage Close

Stage / Batch: P7 / P7-RUNTIME and stage close

HEAD / Commit:

- Pre-batch HEAD: `bfd6f75b` (`P7-HTTP/STATE`).
- Runtime checkpoint: `b615d47c refactor(p7): migrate subscription task runtime`.
- Stage-close documentation: the commit containing this entry; no amend, push, deploy, or release.

Implemented:

- Completed Cleanup durable execution and cancellation recovery, including frozen snapshots,
  Job/Attempt/Outbox state, claim/lease fencing, atomic quarantine, effect receipts, authoritative
  readback, retry budget, and recovery after Worker restart.
- Added Subscribe Assistant persistent snapshot claims and migration
  `0015_p7_assistant_snapshot_claims`; timeout, retry, cancellation, stale Attempt, lease renewal,
  maintenance checkpoints, and restart recovery no longer depend on process memory.
- Added 14 native P7 task envelopes plus Worker/Coordinator process wiring. MoviePilot writes and
  deletes now use durable operation identity, stable occurrences, typed outcomes, and explicit
  `failed`/`partial`/`indeterminate` semantics without duplicate external effects.
- Closed review findings for task ownership cross-checks, Cleanup message-only HTTP responses, and
  async deletion UI wording. Updated Manifest/OpenAPI and synchronized the ordered task fixture from
  current AST definitions.

Targeted gate:

- P7 application/architecture/worker verification -> `273 passed`, one existing Starlette WSGI
  deprecation warning.
- Disposable PostgreSQL 18 P7 suite with `ETK_TEST_DATABASE_URL` unset -> `108 passed`, one existing
  warning; covers retry, cancel, timeout, readback, quota, Outbox, claim, leases and restart recovery.
- Cleanup cancellation and MoviePilot/provider focused reviews -> clean; no reachable P0/P1/P2.
- Detailed targeted artifacts: `/tmp/etk-refactor-artifacts/p7-runtime/` and
  `/tmp/etk-refactor-artifacts/p7-runtime-review-fixes/`.

Stage full gate:

- Ruff -> clean; Ruff format -> `457 files already formatted`; Pyright -> `0 errors, 0 warnings`;
  compileall and `git diff --check` -> passed.
- `pytest -p no:cacheprovider verification -q` -> `1524 passed, 345 skipped, 1 warning`; opt-in skips
  are not counted as PostgreSQL or Redis/Celery evidence.
- Disposable PostgreSQL 18 selected suites (`p3`, `p5`, `p6`, `p7`, `platform`) ->
  `293 passed, 1 warning`.
- Disposable Redis/Celery process-path suite -> `11 passed in 114.73s`.
- Manifest and OpenAPI currentness -> clean; all three fixture/contract secret scans -> clean.
- Frontend production build -> passed, `14940 modules transformed`.
- Final independent P7 integration review -> clean; remaining reachable P0/P1/P2 findings: 0.
- Current evidence: `/tmp/etk-refactor-artifacts/p7-stage-close-20260827/` plus
  `/tmp/etk-refactor-artifacts/p7-runtime-review-fixes/final2-redis-celery.log`.

Pending:

- Begin `P8-LOCAL`: collections, covers, users, discovery, logs/statistics, permissions and fixed
  rendering fixtures.
- Real Emby, 115, MoviePilot, shared center, Telegram and HDHive acceptance remains
  `HUMAN_PENDING` and was not used as local evidence.

Blocker: none. Non-blocking coverage note: migration `0015` has head/schema/FK/check/claim coverage
but no revision-local downgrade/re-upgrade test; the stage review found no migration defect.

Next: execute `P8-LOCAL`, then `P8-EFFECTS`; do not reopen P7 without a new deterministic failure or
review finding.

Safety boundary:

- No real external system, production database, old `/config`, credential, external write, push,
  deployment, release, `web_app.py`, or PyArmor runtime was used.

## P8

### P8-LOCAL

Stage / Batch: P8 / P8-LOCAL

HEAD / Commit:

- Pre-batch HEAD: `b872ab5f` (`docs(p7): close subscription migration`).
- P8-LOCAL checkpoint: the commit containing this entry; no amend, push, deploy, or release.

Implemented:

- Added 47 local Collection/Cover routes with `collections_info` / `custom_collections` ownership,
  cross-domain projections, atomic settings merge, deadlock-safe ordering, and nine PostgreSQL 18
  repository invariants. Eleven Collection provider/effect routes remain on compatibility for the next
  batch.
- Added 12 local Users routes for templates, invitations, expiration, Portal account/history/chat/stats;
  fixed invitation cascade and non-FK user activity cleanup with JSONB, rollback, and projection gates.
- Added four local Discover routes, four local media-search/auto-tag configuration routes, and three
  local review/subscription-stat routes. Subscription stats use the P7 read-only quota snapshot and do
  not mutate `subscription_quota_state`; review storage failures now surface as `500`.
- Added native user-activity projection handling for nine activity events plus durable policy intents.
  Exact favorite, aggregate playback state, confirmed receipt, and downstream Outbox intents share one
  UoW; duplicate/conflicting EventId, concurrent partial patch, and injected rollback are covered. The
  default future-event DI remains legacy until P8-EFFECTS supplies external resolvers/executors.
- Added a deterministic in-memory renderer for `single_1`, `single_2`, `multi_1` gradient/blur, badge,
  missing-poster and cache identity/result, with immutable font/source digests and strict RGBA fixtures.
- Updated 70 Route bindings/decorators, eight Table contracts and four settings contracts through the
  ownership override, then mechanically regenerated clean manifests. Updated the P3 subscription-stat
  exclusion note to its P8 read-only owner.

Targeted gate:

- Scoped Ruff -> passed; Ruff format -> `51 files already formatted`; scoped Pyright ->
  `0 errors, 0 warnings`; compileall and `git diff --check` -> passed.
- P8 application/architecture/unit plus the updated P3 database contract -> `44 passed`, with one
  existing Starlette WSGI deprecation warning. This includes strict RGBA equality for nine cover tests.
- Digest-pinned disposable PostgreSQL 18 P8 suite with `ETK_TEST_DATABASE_URL` unset -> `21 passed`;
  no PostgreSQL skip is counted as evidence.
- Manifest currentness -> clean.
- Detailed output: `/tmp/etk-refactor-artifacts/p8-local-20260827/`.

Stage full gate:

- Not run by batch cadence. Full repository Ruff/Pyright/pytest, cross-stage PostgreSQL 18,
  Redis/Celery, Manifest/OpenAPI, secret scans, frontend build, focused transaction review and P8
  integration review remain pending until P8 stage close.

Pending:

- `P8-EFFECTS`: resolve the invitation-registration and Telegram-token P0 findings; add external
  Collection/Identity/Discover/Emby/Telegram/cover Ports and durable operations; migrate system update,
  database maintenance, review reprocess, 14 task envelopes, Worker/Coordinator wiring and Redis/Celery
  process paths using Fake/Stub only.
- After P8-EFFECTS, run the single P8 stage-close full gate and reviews before changing P8 to `DONE`.

Blocker: none for P8-LOCAL. The three media helper permission conflict remains an explicit P8-EFFECTS
contract decision; no service credential fallback was introduced in this batch.

Next: create the `P8-LOCAL` checkpoint, then execute `P8-EFFECTS`, starting with the registration P0
and Telegram token redaction P0 before lower-severity adapters or task envelopes.

Safety boundary:

- No real Emby, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old `/config`,
  external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P8-STAGE-CLOSE

Stage / Batch: P8 / stage close

HEAD / Commit:

- `334f630a` (`refactor(p8): migrate secondary domain effects`) remains the sole P8-EFFECTS batch checkpoint.
- `291d23ca` (`fix(p8): align post-checkpoint verification contracts`) records the evidence-backed verification
  repairs; it is not a third P8 batch or checkpoint.

Implemented:

- Closed P8-EFFECTS, including the confirmed Database Maintenance contract and all durable effect/Worker paths.
- Refreshed cross-stage task/catalog/schema expectations exposed by the full gate without changing runtime or
  migration behavior.

Targeted gate:

- Required regressions: `29 passed, 1 warning`; PostgreSQL 18 identity regression: `20 passed`.
- P8-EFFECTS targeted gate: Ruff/format/Pyright clean, application `154 passed`, PostgreSQL 18 `123 passed`,
  Manifest/OpenAPI/fixture secret scan/diff check clean.

Stage full gate:

- Final run: Ruff clean; format `552 files already formatted`; Pyright `0 errors, 0 warnings, 0 informations`;
  compileall clean; default verification `1676 passed, 468 skipped, 1 warning`; PostgreSQL 18 cross-stage
  integration `416 passed, 1 warning`; Redis/Celery `11 passed`; Manifest/OpenAPI and all three secret scans
  clean; frontend build succeeded.
- High-risk transaction review and P8 integration review are clean with the suppression-marker read/receipt
  race retained as a non-blocking residual.

Pending:

- Frontend typecheck was not run: `frontend/package.json` has no `typecheck` script and no `tsc`/`vue-tsc` or
  `tsconfig.json`; this belongs to P10.
- Live Acceptance remains `HUMAN_PENDING`.

Blocker: none for P8 Local Code Complete.

Next: start P9-LEDGER from the repaired P8 head `291d23ca`.

### P8-EFFECTS

Stage / Batch: P8 / P8-EFFECTS

HEAD / Commit:

- Pre-batch HEAD: `23980f8c` (`P8-LOCAL`).
- `334f630a` is the sole P8-EFFECTS batch checkpoint; `291d23ca` contains the
  evidence-backed verification-contract repairs and does not create a third P8 batch.

Implemented:

- Added durable Collection provider operations, suppression/readback handling, user policy/template
  effects, identity registration/avatar/status/template operations, Discover/secondary adapters,
  Portal subscription/playback effects, review reprocess, and Worker/Celery routing. External calls
  are typed Ports with local HTTPX Fake/Stub contracts.
- Added bounded backup preview/import/export and durable database maintenance operations. The confirmed
  contract uses 64 MiB compressed, 256 MiB expanded, 20x ratio, 24h artifact TTL, immutable preview
  artifacts, artifact-only import, synchronous gzip export, durable 202 maintenance actions, fixed
  409 active conflicts, protected-FK rejection, overwrite denylist, version/fingerprint metadata and
  `restart_required=true` without automatic restart.
- Added migrations `0016_p8_identity_operations` and `0017_p8_effects_and_maintenance`, ownership
  overlay entries, P8 HTTP fixtures and Greenfield table ownership evidence. `system-auto-update`
  remains native `SKIPPED` and never invokes Docker or a legacy updater.

Targeted gate:

- `uv run ruff check src verification scripts` -> clean.
- `uv run ruff format --check src verification scripts` -> 552 files already formatted.
- `uv run pyright` -> 0 errors, 0 warnings, 0 informations.
- P8 application/architecture/contracts plus P5 media-effect regression -> `154 passed`, one existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 P8 integration -> `123 passed`.
- `python3 scripts/refactor/check_manifests.py`, OpenAPI `--check`, fixture secret scan and
  `git diff --check` -> clean.
- Findings-first review is recorded in `/tmp/etk-refactor-artifacts/p8-effects-20260827/findings-first-review.md`;
  no actionable P0/P1/P2 finding remains. The suppression-marker read/receipt race is documented as
  a non-blocking residual pending reproducible evidence.

Stage full gate:

- Passed after the batch: Ruff/format/Pyright/compileall clean; default verification `1676 passed,
  468 skipped, 1 warning`; PostgreSQL 18 cross-stage `416 passed, 1 warning`; Redis/Celery `11 passed`;
  Manifest/OpenAPI/secret scans clean; frontend build succeeded; focused transaction and integration
  reviews clean with the documented suppression-marker race as a non-blocking residual.

Pending:

- Frontend typecheck remains pending until P10 because no `typecheck` script, `tsc`/`vue-tsc`, or
  `tsconfig.json` exists. Live Acceptance remains `HUMAN_PENDING`.

Blocker: none.

Next: continue in order with P9-LEDGER, P9-ADAPTERS and P9-PROXY.

Safety boundary:

- No real Emby, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old `/config`,
  external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P8-POST-CLOSE-FIX

Stage / Batch: P8 / post-checkpoint evidence fix

HEAD / Commit:

- `29834f1f` (`fix(p8): align maintenance artifact timestamps`) is a post-checkpoint repair and does not
  create a third P8 batch.

Implemented:

- Database Maintenance artifact insertion now persists `created_at` from the same UTC clock used to derive
  the 24-hour `expires_at`. This preserves the immutable artifact contract and satisfies the PostgreSQL
  `expires_at > created_at` constraint under deterministic test clocks.

Targeted gate:

- Database Maintenance PostgreSQL 18 regression -> `3 passed`.
- Ruff for the changed module and fixture -> clean; Pyright for the changed module -> `0 errors, 0 warnings,
  0 informations`; `git diff --check` -> clean.
- `npm --prefix frontend run typecheck` and `npm --prefix frontend run build` -> passed; the build/typecheck
  include `frontend/src/components/UserList.vue` (15,005 modules transformed, existing large-chunk warning).

Stage full gate:

- The single P8 stage-close full gate was already completed before this evidence-backed repair; it was not
  rerun under the compressed cadence. No runtime contract outside artifact timestamp persistence changed.

Pending:

- None for P8 Local Code Complete. Live Acceptance remains `HUMAN_PENDING`.

Blocker: none.

Next: continue P11 pre-removal only after recording the current ownership and native Worker blockers.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old `/config`,
  external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

## P9

### P9-LEDGER

Stage / Batch: P9 / P9-LEDGER

HEAD / Commit:

- Batch checkpoint is the commit containing this entry; no amend, push, deploy, or release.

Implemented:

- Added `sharing_operations` with deterministic operation identity, scope conflict fencing, claim/lease
  recovery, terminal state transitions and identifier-only transactional Outbox payloads.
- Added `sharing_credit_accounts` and append-only `sharing_credit_ledger`. Account-row locking makes
  concurrent debits atomic; idempotent replay returns the original entry, conflicting reuse is rejected,
  and compensation is an explicit positive Ledger entry.
- Added `SharingLedgerService`, in-memory Fake coverage, and a PostgreSQL 18 migration/integration slice.
  Durable request payloads reject secret fields.

Targeted gate:

- Sharing application tests: `5 passed`.
- PostgreSQL 18 sharing migration/Ledger tests: `2 passed`.
- Scoped Ruff, format, Pyright and compileall: clean.

Stage full gate:

- Pending until P9-ADAPTERS and P9-PROXY are complete.

Pending:

- Shared-center, Telegram and HDHive typed adapters, native route/effect wiring, then isolated ASGI
  HTTP/WebSocket Proxy with virtual IDs, pagination, leases and resource cleanup.

Blocker: none.

Next: P9-ADAPTERS.

Safety boundary:

- Only local Fake/Stub and disposable PostgreSQL 18 were used; no real external system, old `/config`,
  production database, external write, push, deployment or release was used.

### P9-STAGE-CLOSE

Stage / Batch: P9 / stage close

Status: LOCAL_DONE

HEAD / Checkpoints:

- `5e2ff150` (`refactor(p9): add contribution ledger`)
- `4d73a33c` (`refactor(p9): migrate sharing adapters`)
- `ed5f3e6e` (`refactor(p9): migrate sharing proxy`)
- `355c4625` (`fix(p9): align cross-stage verification contracts`)
- `7a95ad1d` (`fix(p9): rearm sharing operation outbox`)

Implemented and reviewed:

- Sharing contribution Ledger uses account-row locking, append-only entries, deterministic idempotency and explicit compensation; sharing operations enforce scope fencing, claim leases, terminal state transitions and identifier-only Outbox payloads.
- Shared Center, Telegram and HDHive typed adapters use bounded transport policy; reads may retry, mutations do not replay, and transport/timeout/5xx/invalid JSON outcomes remain rejected, retryable or indeterminate as appropriate. Four P9 native Worker task types are registered exactly once.
- Independent ASGI Proxy covers virtual library IDs, visibility, sorting/pagination, playback redirects, non-ETK rejection, WebSocket behavior and persistent playback leases with advisory-lock concurrency, identity refresh, release and expiry reclaim.
- Fixed sharing Outbox claim/terminal completion and retry/lease-expiry rearm in one transaction; PostgreSQL 18 regression covers pending/processed transitions and stale-claim recovery.

Targeted and stage gates:

- P9 application + PostgreSQL 18 tests: `36 passed`, one expected Starlette WSGI deprecation warning.
- Complete PostgreSQL 18 integration: `474 passed, 11 skipped` (only explicit Redis/Celery opt-in tests skipped in this command).
- Disposable PostgreSQL 18 + Redis 7 + Celery process gate: `11 passed`.
- Full application suite: `1464 passed`, one expected warning; contracts/architecture: `89 passed`, one expected warning.
- Scoped Ruff check/format, Pyright, compileall, Manifest/OpenAPI and fixture secret scans are clean; OpenAPI remains `256 paths / 303 operations`.
- Review result: no P0/P1/P2 blocker. Real Emby/115/MoviePilot/shared center/Telegram/HDHive and production systems remain outside scope; Live Acceptance stays `HUMAN_PENDING`.

Next: start P10 only after the P3-P9 backend contract baseline is frozen.

## P10

### P10-FOUNDATION / PAGE-FAMILY TRANSPORT

Stage / Batch: P10 / foundation and page-family transport migration

HEAD / Commit:

- Work started from `58e27e6f` (`docs(p9): close sharing migration`); no amend, push, deploy,
  or release.

Implemented:

- Frozen `verification/contracts/http/p4-openapi.json` is the sole OpenAPI input. Added a
  deterministic `$defs` normalizer and generated `frontend/src/api/generated/schema.ts` with
  `openapi-typescript`; generated output remains hands-off.
- Added the `openapi-fetch` typed client, a single credentials/envelope/error adapter with bounded
  timeout support, and explicit upload/download/stream helpers. API path literals are isolated in
  the compatibility endpoint registry; pages no longer import Axios or call native `fetch`.
- Added Vue Query bootstrap, query/mutation helpers, and migrated release information to a server
  query. Pinia app state is now client-only UI state; auth remains the only server-backed Pinia store.
- Migrated all 55 Vue SFCs to `<script setup lang="ts">`, renamed frontend modules to TypeScript,
  preserved Router paths/guards and existing upload/download/streaming behavior, and added the
  API-boundary scan.
- Added Vitest adapter coverage and stubbed Playwright E2E coverage for login/setup plus config,
  task, organize, subscription, sharing and user-center routes. Browser tests use local Vite and
  fake responses only.

Targeted gate:

- `npm ci` completed (`606 packages added`, `10 vulnerabilities: 2 moderate, 8 high`; no
  `npm audit fix` was run); `npm run typecheck` passed; `npm run test:unit` passed (`7 tests`);
  `npm run build` passed (`15005 modules transformed`, Vite 6.4.1, existing large-chunk warning);
  `npm run test:e2e` passed (`8 passed` using local Vite and Fake API); and
  `npm run check:api-boundary` passed (`frontend_api_boundary=clean`).

Stage full gate:

- Frontend stage gate is complete: typecheck, Vitest, build, Playwright E2E and API-boundary scan
  all pass after the final adapter adjustments. `uv run python scripts/refactor/check_manifests.py`,
  `uv run python scripts/refactor/export_openapi.py --check`, fixture secret scan and
  `git diff --check` remain clean from the P10 manifest repair verification.

### P10-STAGE-CLOSE

Stage / Batch: P10 / stage close

HEAD / Commit:

- Frontend implementation checkpoints: `3468296d`, `cf6f3d2e`, `8bfa9b51`, `0eb634eb`.
- Final transport-cancellation fixes and E2E public-route stub correction are checkpointed before
  this documentation close; no amend, push, deploy, or release.

Implemented:

- `ApiError` now preserves legacy cancellation codes, timeout aborts are deterministic, and an
  external `AbortSignal` is combined with adapter timeout handling instead of leaving the timeout
  controller detached.
- The generated `openapi-fetch` client explicitly uses `credentials: include`; all page requests
  remain behind the endpoint registry/adapter boundary, with native fetch limited to adapter and
  explicit stream/raw helpers.
- The unauthenticated login E2E stub now returns `login_required` and asserts route reachability;
  setup and authenticated route checks continue to use local Fake responses.

Targeted gate:

- `npm run test:unit` -> `7 passed`; `npm run typecheck` -> passed; `npm run build` -> passed;
  `npm run test:e2e` -> `8 passed`; `npm run check:api-boundary` -> `frontend_api_boundary=clean`.
- Generated `frontend/src/api/generated/schema.ts` remains deterministic and hands-off; route
  manifest and OpenAPI currentness checks remain clean.

Findings-first review:

- API adapter error/timeout/invalid JSON, FormData boundary handling, blob download, stream/raw
  escape hatches and credentials were reviewed; the cancellation/timeout issue above was fixed and
  covered by unit tests.
- Auth guard paths and public/login/setup behavior match the pre-migration router; route manifest
  parity is clean. No direct Axios/native fetch or unregistered `/api/` literal remains outside the
  adapter boundary. Pinia retains only auth and client UI state; server config/release state uses
  Vue Query.
- No actionable P0/P1/P2 finding remains. Existing npm audit vulnerabilities and Vite large-chunk
  warning are residuals outside this migration scope; no dependency upgrade was introduced.

Pending:

- P11 pre-removal ownership and legacy removal remain pending. Live Acceptance remains
  `HUMAN_PENDING`.

Blocker: none. E2E browser binaries are installed locally; no real backend or external service is used.

Next: start P11-Pre-removal; do not remove `web_app.py` or execute legacy runtime code.

Safety boundary:

- No real Emby, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment or release was used.

## P11

### P11-Pre-removal

Stage / Batch: P11 / P11-Pre-removal

HEAD / Commit:

- P11 pre-removal task slice checkpoint: `2f181fd6` (`refactor(p11): migrate organize record tmdb
  backfill`). The controlled removal checkpoint was not created because the required 100% ownership
  disposition and native Worker execution graph are still incomplete.

Implemented:

- Native API startup now omits the WSGI fallback by default, uses the Flask-compatible signed
  session format without importing Flask, and keeps compatibility opt-in behind
  `ETK_ENABLE_LEGACY_COMPATIBILITY=1`.
- Native Proxy uses the ASGI implementation; Coordinator and Worker have explicit native-profile
  branches; the refactor image excludes the legacy Processor, TaskManager, SchedulerManager,
  extensions, routes, database, tasks, services, `web_app.py`, and PyArmor runtime sources.
- Generated the full pre-removal inventory at
  `/tmp/etk-refactor-artifacts/p11-pre-removal-20260828/` with route/task/table/config/frontend
  counts, per-entry disposition, dependency/import scan, image content list, removal waves,
  rollback notes, and the explicit `web_app.py`/PyArmor read boundary.

Targeted gate:

- `uv lock --check` -> passed.
- P11 scoped Application gate (`test_app_factory_contract.py`, `test_nginx_and_proxy.py`,
  `test_import_and_cli.py`, `test_process_roles.py`) -> `46 passed, 1 warning`.
- Required future-domain regressions (`test_media_effect_process.py`, `test_identity_process.py`,
  `test_collection_operation_process.py`) -> `29 passed`.
- Required Identity PostgreSQL 18 regression -> `20 passed`.
- Scoped Ruff check/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`);
  `git diff --check` -> passed.
- Native `create_app(start_runtime=True)` smoke with an isolated in-memory container -> health
  `200`; forbidden modules loaded `[]`.
- Latest verification artifact: `/tmp/etk-refactor-artifacts/p11-pre-removal-20260828/latest-verification-20260828-task-backfill.md`.
- After the native-profile test updates, `verification/architecture` -> `73 passed, 1 warning`; P8
  application -> `135 passed`; Database Maintenance PostgreSQL 18 -> `3 passed`; manifests and
  OpenAPI checks are clean. Native-only Worker now terminalizes a non-native task as
  `LegacyTaskAdapterUnavailable` instead of leaving a claimed attempt running.
- Resume verification on 2026-08-28 reran the repository suite: `1706 passed, 474 skipped, 1
  warning`. The first rerun exposed the existing Flask-context supervisor injection contract when
  the native flag was unset; `get_runtime_supervisor()` now inspects an already-loaded Flask module
  through `sys.modules` without importing Flask in native mode, and the focused test plus full suite
  pass.
- The former sole legacy task, `backfill-organize-record-tmdb-ids`, now has a native P6
  `P115OrganizeRecordTmdbBackfillService`, account-local PostgreSQL PickCode/SHA1 resolution, a
  strict `P6P115TaskRouter` envelope, Worker wiring, and ownership/ordered-task catalog evidence.
  The generated task manifest now reports `50 new / 0 legacy`.

Stage full gate:

- Not run. The removal wave is not authorized while the pre-removal ownership and native Worker
  execution blockers remain.

Pending:

- Route ownership is `343` decorators (`278 new / 65 legacy`) and `367` method bindings
  (`295 new / 72 legacy`); all 72 remaining legacy method bindings have no matching operation in the current
  native OpenAPI surface (`256 paths / 303 operations`).
- Task ownership is now `50 new / 0 legacy`; table ownership before the table slice below was
  `15 new / 24 legacy`;
  config ownership is `14 new / 89 legacy` plus `11 new / 42 legacy` standalone settings;
  frontend route ownership is now `25 new / 0 legacy`, with each route explicitly owned by the
  TypeScript router and validated by the P10 frontend gate.
- The native-only `WorkerRuntime._start_native_only()` still initializes only the durable control
  plane and sets `_adapter=None`; native task routers/executors are not yet installed for a full
  execution graph. P5/P7/P8 routers also still use compatibility task callables for part of the
  50-key catalog.
- The complete per-entry list and import evidence are in the pre-removal JSON/Markdown artifacts;
  no legacy endpoint was silently marked `retired-approved` because the existing plan preserves
  public URL/method contracts and contains no retirement approval for these entries.

Blocker:

- P11 remains blocked by unresolved native owner/approved-retirement decisions for 65 route
  decorators, 72 method bindings, 15 tables, 89 config keys, and 42 standalone settings, plus the
  missing native Worker task execution graph and compatibility-boundary imports. The former task
  blocker is closed. Removing compatibility code now would either drop existing URL/method behavior
  or make durable jobs fail before execution.

Next:

- Add native implementations or explicit, evidence-backed retirement approvals for every remaining
  listed entry; the frontend route inventory and task catalog are already resolved. Then build and
  test the complete native Worker execution graph before starting the single controlled removal
  wave. P12 remains TODO until P11 removal and NFR gates pass.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Table-Ownership

Stage / Batch: P11 / table-ownership slice

HEAD / Commit:

- Pre-slice HEAD: `3992337b` (`docs(p11): record pre-removal task blocker`).
- Checkpoint is created only after the targeted gate below; this slice does not authorize removal or
  create a second P11 stage.

Implemented:

- Assigned native owners and evidence-backed PostgreSQL 18 fixtures to nine previously unowned tables:
  `actor_subscriptions`, `app_settings`, `cleanup_index`, `failed_log`, `media_image_policy_cache`,
  `media_metadata`, `resubscribe_index`, `resubscribe_rules`, and `subscribe_assistant_state`.
- Regenerated the table manifest from the ownership overlay. The inventory now reports `24 new / 15 legacy`
  tables; no table was marked `new` without an existing repository and integration fixture.

Targeted gate:

- `uv run ruff check src verification scripts/refactor` -> clean; format check -> `578 files already formatted`.
- `uv run pyright` -> `0 errors, 0 warnings, 0 informations`.
- Required future-domain application regressions -> `29 passed`; Identity PostgreSQL 18 regression ->
  `20 passed`.
- `uv run python scripts/refactor/export_manifests.py` and `check_manifests.py` -> clean; `git diff --check`
  -> clean.
- Findings-first P8 review found no new actionable P0/P1/P2; the collection suppression-marker read/receipt
  race remains the documented non-blocking residual.

Stage full gate:

- Not run. This is a bounded ownership slice; P11 removal and NFR/security/SBOM/license gates remain blocked
  by unresolved route/config/setting ownership and the native Worker graph.

Pending:

- Fifteen legacy tables remain without native owners, alongside `65` route decorators, `72` method bindings,
  `89` config keys, `42` standalone settings, and compatibility-boundary imports.
- Native-only Worker still has no complete router/executor graph; P5/P7/P8 paths retain compatibility
  callable fallbacks for part of the catalog.

Blocker: P11 remains blocked; this slice only reduces the table inventory and does not permit the controlled
removal wave.

Next: continue with evidence-backed ownership slices and the native-only Worker execution graph; do not mark
P11 `DONE` or start P12 until all pre-removal blockers and P11 NFR gates pass.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old `/config`,
  external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-RESUME-20260828

Stage / Batch: P11 / pre-removal resume verification

HEAD / Commit:

- Current implementation HEAD: `29834f1f82413e13f445be31cae1a41ee2a1a565`.
- `5e28d662` aligned the media ownership fixture; `29834f1f` fixed the evidence-backed Database Maintenance
  artifact timestamp regression. Neither change authorizes a removal wave or creates a new P11 stage.

Implemented:

- Refreshed the latest pre-removal evidence against the current manifests: route decorators `278 new / 65 legacy`,
  method bindings `295 new / 72 legacy`, tasks `50 new / 0 legacy`, tables `24 new / 15 legacy`, config keys
  `14 new / 89 legacy`, standalone settings `11 new / 42 legacy`, and frontend routes `25 new / 0 legacy`.
- Kept all unresolved public routes, tables, configuration entries and settings explicit; no retirement approval
  or replacement owner was inferred from path existence alone.

Targeted gate:

- Required P8 application regressions -> `29 passed`; Identity PostgreSQL 18 -> `20 passed`.
- P8 application -> `135 passed`; Database Maintenance PostgreSQL 18 -> `3 passed` after the timestamp fix.
- P11 architecture -> `73 passed, 1 warning`; P11 application scope -> `46 passed, 1 warning`.
- Ruff, format, Pyright, Manifest, OpenAPI and `git diff --check` -> clean. Warnings are the existing Starlette
  `WSGIMiddleware` deprecation.

Stage full gate:

- Not run. The single P11 removal/NFR gate remains unauthorized while ownership and native Worker blockers remain.

Pending:

- Native-only `WorkerRuntime._start_native_only()` still initializes only the durable control plane and sets
  `_adapter=None`; the complete native P5/P7/P8 execution graph is not installed.
- Native P5/P7/P8 routers still call compatibility task callables for part of the catalog, and native source
  still contains compatibility-boundary imports (`task_manager`, `extensions`, `config_manager`, `requests`,
  Flask/WSGI).
- The remaining 65 route decorators, 72 method bindings, 15 tables, 89 config keys and 42 standalone settings
  lack native owner or evidence-backed retirement approval.

Blocker:

- P11 remains blocked by the unresolved ownership contracts and missing native Worker graph. Removing the
  compatibility runtime now would either drop existing URL/method behavior or leave durable jobs unable to
  execute. P12 Greenfield destroy/rebuild is therefore not started.

Next: retain P11 as `BLOCKED`; continue only with a concrete native owner/contract implementation or explicit
retirement approval, then build the complete native Worker graph before the one controlled removal wave. Keep
Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old `/config`,
  external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-Worker-Foundation

Stage / Batch: P11 / native Worker execution foundation

HEAD / Commit:

- Pre-slice HEAD: `7f445cf6` (`docs(p8): record maintenance and frontend verification`).
- Checkpoint: this batch's `refactor(p11): wire processor-free worker execution` commit after the
  targeted gate below.

Implemented:

- Native-only Worker startup now wires the processor-free P6 ingestion scan, P8 user-sync and
  native-disabled system update, and all four typed P9 sharing task paths without a legacy task
  adapter or `task_manager` execution context.
- Native-only Worker startup now wires the durable Subscription, Identity, and Database Maintenance
  operation executors. Startup failure stops the partially started supervisor and closes the
  container; shutdown clears the installed fields.
- `_NATIVE_ONLY_EXECUTABLE_TASK_KEYS` is the explicit execution truth for this bounded slice.
  Every registered task or P8 review task outside that set terminalizes as
  `NativeTaskExecutionUnavailable`; a native envelope is no longer treated as proof of a native
  execution owner.
- Task chains now reject unknown or not-yet-migrated children and fail the parent after a failed
  child instead of returning `completed`.
- The stale one-legacy-task assertion was replaced with the current `50 native / 0 legacy` envelope
  assertion plus exhaustive negative execution coverage for every catalog/review task outside the
  native-only executable set. All routers, the legacy adapter, processor resolution, and
  `task_manager` import are failure sentinels in that test.

Targeted gate:

- `uv run ruff check src verification scripts/refactor` -> clean; format check -> `578 files already
  formatted`.
- `uv run pyright` -> `0 errors, 0 warnings, 0 informations`.
- Worker/router application scope -> `156 passed, 1` existing Starlette WSGI deprecation warning.
- Required future-domain application regressions -> `29 passed`.
- Identity PostgreSQL 18 -> `20 passed`; Database Maintenance PostgreSQL 18 -> `3 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest and OpenAPI checks -> clean; `git diff --check` -> clean.
- Findings-first Worker review initially found incomplete execution truth and task-chain false-success
  paths. After the explicit allowlist, exhaustive negative tests, and chain fixes, focused rereview
  found no remaining P0/P1/P2 in this slice.

Stage full gate:

- Not run. This checkpoint establishes an honest processor-free execution foundation; it is not the
  P11 controlled removal or NFR checkpoint.

Pending:

- Native P6 115, P5 MediaInfo restore, P7 tasks, generic P8/review tasks, Media Effects, Collection,
  Cleanup, and P115 upload/playback/intro executors still need real execution owners before they can
  enter the native-only executable set.
- Route decorators/method bindings, the remaining tables, dynamic config keys, standalone settings,
  and compatibility-boundary imports remain explicit pre-removal work. No ownership count changed
  in this slice.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory, and P12 remain pending.

Next: continue with evidence-backed native execution and ownership slices. Do not start the removal
wave until all ownership and execution entries have real owners or approved retirements.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Standalone-Settings-Ownership

Stage / Batch: P11 / standalone settings ownership

HEAD / Commit:

- Pre-slice HEAD: `9a72f2e9` (`refactor(p11): wire processor-free worker execution`).
- Checkpoint: this batch's `refactor(p11): assign standalone settings ownership` commit after the
  targeted gate below.

Implemented:

- Assigned `custom_theme` and `custom_menu_config` to
  `SqlAlchemyAppSettingsRepository`, with `StandaloneSettingsService` retained as the explicit
  semantic owner for save/delete/read/reset behavior.
- Regenerated `config-ownership.yaml` from `ownership-overrides.json`; standalone settings moved
  from `11 new / 42 legacy` to `13 new / 40 legacy`. Dynamic config remains `14 new / 82 legacy`
  plus seven legacy bootstrap keys.
- Added explicit manifest assertions and a PostgreSQL 18 JSONB round-trip/delete test. The PG18 test
  snapshots and restores any pre-existing rows, including `last_updated_at`, and always shuts down
  its runtime; an injected test database is not left mutated.

Targeted gate:

- `uv run ruff check src verification scripts/refactor` -> clean; format check -> `579 files already
  formatted`.
- `uv run pyright` -> `0 errors, 0 warnings, 0 informations`.
- System standalone plus required future-domain application scope -> `47 passed`.
- New standalone settings PostgreSQL 18 plus Identity PostgreSQL 18 -> `21 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest and OpenAPI checks -> clean; `git diff --check` -> clean.
- Focused review found one P2 in the first PG18 cleanup: it deleted possible pre-existing settings.
  The test now snapshots/restores exact JSONB and timestamps under nested cleanup/shutdown `finally`;
  rereview found no remaining P0/P1/P2.

Stage full gate:

- Not run. This is an evidence-backed pre-removal ownership slice, not the controlled removal/NFR
  checkpoint.

Pending:

- Forty standalone settings remain legacy-owned. Settings whose defaults or consumers still import
  compatibility code were not mechanically promoted.
- Route/method ownership did not change. The largest residual group, `routes/shared_resource.py`
  (`29` decorators / `31` bindings), has no one-to-one native HTTP/service/PG18 replacement and
  therefore remains explicit rather than being retired or relabeled.
- The remaining Worker graph, tables, config keys, route contracts, compatibility imports,
  controlled removal, NFR reports, and P12 remain pending.

Next: implement real owners for one residual route or execution group; do not infer ownership from a
similar native path or a typed settings declaration alone.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Emby-Search-Index

Stage / Batch: P11 / native Emby search-index ownership slice

HEAD / Commit:

- Pre-slice HEAD: `3a69e287` (`refactor(p11): assign standalone settings ownership`).
- Checkpoint: this batch's `refactor(p11): migrate native emby search index` commit after the
  targeted gate below.

Implemented:

- Replaced the active legacy owners for `GET /api/emby/search` and
  `POST /api/emby/search/index` with `EmbySearchIndexService` and
  `SqlAlchemyEmbySearchRepository`, while retaining the exact empty-query, no-store, readiness,
  rebuild, batch, incremental, delete and error envelopes.
- Preserved the existing Chinese, pinyin full/initial, ngram, type-priority and Series-child
  suppression ranking semantics. A punctuation-only query now follows the legacy compact-empty
  fast path instead of evaluating an empty PostgreSQL substring.
- Assigned `emby_search_index` and `emby_search_rebuild_state` to the native repository. Rebuild
  start cleanup, item upsert/delete, and rebuild completion cleanup each use explicit SQLAlchemy
  units of work; stale-index deletion and terminal rebuild-state deletion commit together.
- Unified the overlapping handoff tests into the P5 media application/integration layout and added
  native-before-compatibility, import-sentinel, ownership-manifest, sanitized HTTP fixture and
  PostgreSQL 18 evidence. Route ownership is now `280 new / 63 legacy`, method bindings
  `297 new / 70 legacy`, and tables `26 new / 13 legacy`.
- Regenerated Route/Table manifests and the compatibility OpenAPI snapshot; the current surface is
  `258` paths / `305` operations.

Targeted gate:

- `uv run ruff check src verification scripts/refactor` -> clean; format check -> `582 files already
  formatted`.
- `uv run pyright` -> `0 errors, 0 warnings, 0 informations`.
- Search plus required future-domain application regressions -> `35 passed`, with the existing
  Starlette WSGI deprecation warning only.
- Disposable PostgreSQL 18 search, Identity and Database Maintenance regression scope ->
  `25 passed`.
- Architecture -> `73 passed`, with the existing Starlette WSGI deprecation warning only.
- Manifest and OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Findings-first review covered ranking and child suppression, rebuild readiness/concurrency,
  mutation UoWs, route precedence, fixture provenance and ownership IDs. Initial fixture-schema,
  compact-empty and fixture-secret findings were fixed; no P0/P1/P2 remains in this slice.

Stage full gate:

- Not run. P11 still has explicit route, table, config, Worker and compatibility-runtime ownership
  gaps; this checkpoint does not authorize controlled removal or P12.

Pending:

- Seventy method bindings, 63 route decorators and 13 tables remain legacy-owned. The remaining
  configuration ownership, native Worker execution graph, compatibility imports, controlled
  removal, NFR reports, SBOM, vulnerability and License evidence remain pending.

Next: wire the already migrated `backfill-organize-record-tmdb-ids` service into the native-only
Worker, then continue residual webhook/P115/subscription/shared-resource ownership without treating
typed or skipped legacy adapters as native execution evidence.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Organize-Record-Backfill-Worker

Stage / Batch: P11 / organize-record backfill Worker slice

HEAD / Commit:

- Pre-slice HEAD: `99468a83` (`refactor(p11): migrate native emby search index`).
- Checkpoint: this batch's `refactor(p11): execute native organize record backfill` commit after the
  targeted gate below.

Implemented:

- Added `backfill-organize-record-tmdb-ids` to the explicit native-only executable task set. The
  Worker now constructs an isolated `P6P115TaskRouter` with only
  `P115OrganizeRecordTmdbBackfillService` and `SqlAlchemyP115OrganizeRecordsRepository`; it does not
  construct credentials, provider, directory-sync or organize-mutation adapters for this path.
- Made the P6 router's per-use-case dependencies optional and added branch-local unavailable checks,
  so one proven processor-free use case can be wired without falsely making the other eight P115
  tasks executable.
- Added a normal success-path import sentinel that rejects `task_manager`, `handler.*`, `routes.*`
  and legacy `database.*` imports during execution, plus native startup graph and exhaustive
  allowlist/negative evidence. Native-only execution is now `10 executable / 40 unavailable`
  catalog keys; both P8 review tasks remain unavailable.
- Preserved the existing local-only PostgreSQL identity backfill and progress/result contract. No
  115 provider or other external side effect is part of the task.

Targeted gate:

- `uv run ruff check src verification scripts/refactor` -> clean; format check -> `582 files already
  formatted`.
- `uv run pyright` -> `0 errors, 0 warnings, 0 informations`.
- Worker/router application scope -> `164 passed`, with the existing Starlette WSGI deprecation
  warning only; required future-domain application regressions -> `29 passed`.
- Disposable PostgreSQL 18 organize-record, Identity and Database Maintenance scope -> `26 passed`.
- Architecture -> `73 passed`, with the existing Starlette WSGI deprecation warning only.
- Manifest and OpenAPI checks plus `git diff --check` -> clean.
- Focused review covered execution allowlisting, dependency isolation, import sentinels, local
  mutation idempotency, progress-after-commit retry behavior and negative coverage. One stale
  architecture assertion that assumed a single P6 router construction was repaired; no P0/P1/P2
  remains in this slice.

Stage full gate:

- Not run. Forty catalog tasks, both P8 review tasks, residual ownership, compatibility runtime and
  P11 NFR/security evidence remain pending.

Pending:

- The other eight P6 P115 tasks remain intentionally unavailable because their normal paths still
  touch legacy credentials, provider transports, configuration or effect adapters. Empty, disabled,
  cache-hit or skipped paths are not accepted as migration evidence.

Next: continue with a real residual route/table/config owner. Do not expand the P6 P115 allowlist
until a normal processed path has native provider/readback/effect ownership and legacy-import
sentinels.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P8-Avatar-Artifact-Clock-Fix

Status: DONE

Checkpoint:

- `5e1e1730 fix(p8): anchor avatar artifact creation to operation clock`

Evidence:

- The required PostgreSQL 18 regression exposed six late-day failures because avatar artifact
  `expires_at` used the injected operation clock while database-default `created_at` used wall time.
- `PostgresIdentityOperationRepository` now persists artifact `created_at` from the same operation
  clock; the 24-hour expiry policy is unchanged.
- Identity application scope -> `29 passed`; disposable PostgreSQL 18 Identity scope -> `20 passed`.

### P11-Policy-Defaults-Ownership

Stage / Batch: P11 / plaintext policy-defaults and app-settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `b7893d45` (`refactor(p11): execute native organize record backfill`).
- Checkpoint: this batch's `refactor(p11): assign collection policy defaults ownership` commit
  after the targeted gate below.

Implemented:

- Moved the exact collection/media policy defaults from legacy `utils.py` into the side-effect-free
  plaintext `etk.policy_defaults` module. `utils.py` now re-exports the same objects, preserving all
  existing legacy imports and mutable-object identity.
- Updated Collection, Discover/Media, Resubscribe and P115 native consumers to load those defaults
  directly. Their normalized SHA-256 remains
  `b783ca6ea379306d4b2b06f9b1e70ff1e9c77338320cf37d359d4ad6e828ae26`.
- Assigned native ownership to ten `app_settings` keys: country, junk-file, keyword, language,
  native-collection, rating mapping/priority, release-group, studio and unified-rating settings.
  `CollectionService` retains HTTP/normalization semantics and
  `SqlAlchemyAppSettingsRepository` owns JSONB load/save/merge transactions.
- Standalone settings ownership is now `23 new / 30 legacy`, previously `13 new / 40 legacy`.
  Route, table, dynamic/bootstrap config, Worker and frontend counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `585 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Policy-default, Collection, Resubscribe, P115 and required future-domain application scope ->
  `91 passed`.
- Disposable PostgreSQL 18 collection-settings, Identity and Database Maintenance scope ->
  `24 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Contract/Manifest scope -> `13 passed`; Manifest, OpenAPI, fixture secret scan and
  `git diff --check` -> clean.
- Findings-first review repaired one stale P6 architecture assertion. Exact value drift, legacy
  re-export identity, native import boundaries, JSONB round-trip/merge and cross-domain fallbacks
  were reviewed; no P0/P1/P2 remains.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-policy-defaults-20260828/`.

Stage full gate:

- Not run. This slice reduces standalone-setting and compatibility-default debt; route/table,
  dynamic/bootstrap config, native-only Worker execution, controlled removal and P11 NFR/security
  gates remain.

Process note:

- A read-only P115 audit agent created one untracked, unintegrated `local_config.py` draft contrary
  to its instruction. Provenance was confirmed, both agents were stopped, and only that
  agent-created draft was removed before validation. No concurrent writer or unexplained file
  remains.

Next: migrate one evidence-backed local-only P115 config/read route slice without including provider
mutations or external writes. Keep all unmigrated bindings explicit and do not start controlled
removal until ownership and native execution reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, Telegram, MoviePilot, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Local-Settings

Stage / Batch: P11 / native P115 local-settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `dd100e53` (`refactor(p11): assign collection policy defaults ownership`).
- Checkpoint: this batch's `refactor(p11): migrate local p115 settings` commit after the targeted
  gate below.

Implemented:

- Replaced the active owners for `GET/POST /api/p115/default_stream_config` and
  `GET /api/p115/release_groups` with `P115LocalSettingsService` and the existing
  `SqlAlchemyAppSettingsRepository`; the normal route path does not construct credentials, a 115
  provider or any external adapter.
- Preserved the exact default-stream defaults and normalization, including legacy scalar/non-object
  JSON handling, malformed JSON `400`, unsupported content type `415`, Werkzeug-compatible HTML
  `500`, release-group read-failure fallback and admin denial before body parsing.
- Replaced the MediaInfo Worker's `database.settings_db` policy provider with
  `PersistentP115MediaInfoPolicyProvider(app_settings)`. Language mapping and default-stream policy
  are now captured by one `load_many()` SQLAlchemy snapshot transaction.
- Added isolated legacy golden fixtures, native HTTP/DI/import-boundary assertions and a PostgreSQL
  18 snapshot/restore round-trip. Route ownership is now `282 new / 61 legacy` decorators and
  `300 new / 67 legacy` method bindings; standalone settings are `24 new / 29 legacy`.
- Regenerated the Route/Config manifests and compatibility OpenAPI snapshot. The current surface is
  `260` paths / `308` operations.

Targeted gate:

- Scoped P115 local-settings/MediaInfo/Worker validation -> `66 passed`; scoped Ruff and Pyright ->
  clean (`0 errors, 0 warnings, 0 informations`). A fresh-process normal provider snapshot confirmed
  that `database.settings_db` is not imported.
- Whole-scope Ruff -> clean; format check -> `588 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- P115/local plus required P1/P5/P8 application regressions -> `138 passed, 1` existing Starlette
  WSGI deprecation warning.
- Disposable PostgreSQL 18 local-settings, Identity and Database Maintenance scope -> `24 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Findings-first review covered normalization, JSON/error transport, permission precedence,
  release-group fallback, snapshot atomicity, Worker import boundaries, route precedence and fixture
  provenance; no P0/P1/P2 remains.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-p115-local-settings-20260829/`.

Stage full gate:

- Not run. P11 still has explicit route, table, dynamic/bootstrap config, Worker execution and
  compatibility-runtime gaps; this checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-seven method bindings, 61 route decorators, 13 tables, 82 dynamic config keys, seven
  bootstrap keys and 29 standalone settings remain legacy-owned. Native-only Worker execution
  remains `10 executable / 40 unavailable` catalog keys.
- Washing-priority config/groups/recalculation state and other P115 provider/write routes remain
  explicit. Shared-resource, subscription and webhook ownership also remains.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: migrate the local washing-priority config/groups and recalculation-state slice with native
PostgreSQL ownership before considering provider/write P115 routes. Keep Live Acceptance
`HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Washing-Priority

Stage / Batch: P11 / native P115 washing-priority local ownership slice

HEAD / Commit:

- Pre-slice HEAD: `41245c8a` (`refactor(p11): migrate local p115 settings`).
- Checkpoint: this batch's `refactor(p11): migrate p115 washing priority state` commit after the
  targeted gate below.

Implemented:

- Replaced `GET/POST /api/p115/washing_priority_config` and
  `GET/POST /api/p115/washing_priority_groups` with native FastAPI/service/repository owners while
  preserving exact permission, normalization, migration, JSON and error transport behavior.
- Added `SqlAlchemyP115WashingPriorityRepository` for ordered group reads and atomic group
  replacement. Group mutation and revisioned recalculation-scope merge commit in one UoW, use
  deterministic advisory serialization and do not use `TRUNCATE`.
- Added native config/state save, deterministic affected-scope calculation and revision
  compare-and-ack. Version-slot changes mark Movie and Series for full recalculation; an unchanged
  group replacement does not create an empty state row.
- Rewired rename-config conflict-mode writes to the native service. The active app factory no longer
  constructs `LegacyP115WashingPriorityConfigGateway`.
- Added isolated legacy golden fixtures and PostgreSQL 18 rollback/state/sequence restoration
  evidence. Route ownership is now `284 new / 59 legacy` decorators and `304 new / 63 legacy`
  bindings; standalone settings are `26 new / 27 legacy`; tables are `27 new / 12 legacy`.
- Regenerated Route/Config/Table manifests and the compatibility OpenAPI snapshot at `262` paths /
  `312` operations.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `592 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Washing/P115/app-factory plus required P5/P8 application regressions -> `133 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 washing/local-settings, Identity and Database Maintenance scope ->
  `25 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Findings-first review repaired the migration persistence-shape and no-op state-row differences;
  the final independent review found no remaining P0/P1/P2.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-p115-washing-priority-20260829/`.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-three method bindings, 59 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 27 standalone settings remain legacy-owned. Native-only Worker execution
  remains `10 executable / 40 unavailable` catalog keys.
- `POST /api/p115/washing_priority_recalculate` and its normal processed Worker path remain legacy;
  they were not relabeled based only on native local state ownership.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: migrate the washing-priority recalculation API/Worker only after its normal processed path has
native media-query, MediaInfo/policy and revision-ack ownership; otherwise continue another
evidence-backed local P115/ownership slice. Keep Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Washing-Recalculation

Stage / Batch: P11 / native washing-priority recalculation API and Worker slice

HEAD / Commit:

- Pre-slice HEAD: `d8128be7` (`refactor(p11): migrate p115 washing priority state`).
- Checkpoint: this batch's `refactor(p11): execute native washing priority recalculation` commit
  after the targeted gate below.

Implemented:

- Replaced `POST /api/p115/washing_priority_recalculate` with a native durable submission path:
  empty pending scope remains immediate `200`, fresh jobs return `202`, idempotent replay returns
  the original Job as `200`, and active/different-intent conflicts return `409`. Job payloads contain
  only `item_type`, `limit`, `affected_scope`, and `scope_revision`.
- Added a legacy-compatible plaintext evaluator for ordered normal/exclude rules, MediaInfo parsing,
  language/source/effect/release-group matching, clean-version runtime fallback, version slots,
  target-CID inference/backfill, and exact cache/media snapshot reduction. It does not import the
  legacy handler/task/database/config runtime.
- Added a SQLAlchemy repository with one policy snapshot read, scoped in-library candidate queries,
  per-item UoWs, source-fingerprint and `in_library` drift rejection, cache/media snapshot mutation,
  organize-record CID backfill, and revision compare-and-ack. The optional TMDb runtime read is a
  narrow `httpx` port and all tests use Fake/Mock transports.
- Wired the processor-free service into normal and native-only Workers. Native-only execution is now
  `11 executable / 39 unavailable`; partial row failures map to `TaskOutcome.PARTIAL`.
- Regenerated Route/Task manifests and OpenAPI. Ownership is now `285 new / 58 legacy` route
  decorators and `305 new / 62 legacy` method bindings; OpenAPI is `263 paths / 313 operations`.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `598 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Washing/Worker plus required P5/P7/P8 application regressions -> `192 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 washing state/recalculation, Identity and Database Maintenance scope ->
  `25 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-p115-washing-recalculation-20260829/`.

Focused review:

- Fixed empty-scope idempotent replay, post-candidate `in_library` drift, missing Episode identity
  runtime lookup, and PG18 setup cleanup-boundary findings. Evaluator parity, Job payload/busy
  semantics, per-item UoW, source drift, revision ack, import sentinels and ownership were rereviewed;
  no remaining P0/P1/P2 was found in this slice.
- One read-only audit agent wrote four scoped files contrary to its instruction. Provenance was
  confirmed, the agent was stopped, the generic Job-service expansion was removed, and every
  retained line was independently reviewed and revalidated; no unexplained writer or file remains.

Stage full gate:

- Not run. P11 still has explicit route/config/table/Worker execution and compatibility-runtime
  gaps; this checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-two method bindings, 58 route decorators, 12 tables, 82 dynamic config keys, seven bootstrap
  keys and 27 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain unavailable
  in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the next evidence-backed residual route/config/table or processed Worker execution
slice. Do not start controlled removal until ownership and native execution reach 100%; keep Live
Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Local-Directories

Stage / Batch: P11 / native local-directory browsing slice

HEAD / Commit:

- Pre-slice HEAD: `68e926c5` (`refactor(p11): execute native washing priority recalculation`).
- Checkpoint: this batch's `refactor(p11): migrate local directory browsing` commit after the
  targeted gate below.

Implemented:

- Replaced `GET /api/p115/system/directories` with a native, local-only filesystem service and
  FastAPI route. The normal path does not construct credentials or any Emby, 115, TMDb,
  MoviePilot, Telegram, HDHive or shared-center adapter.
- Preserved the legacy POSIX root, Windows drive, parent entry, sorted visible-directory filtering,
  and `403`/`404`/`500` response contracts. Admin denial occurs before query processing or
  filesystem access, and blocking filesystem calls run in the threadpool.
- Added isolated legacy golden fixtures, real local-filesystem filtering coverage, explicit POSIX
  and mocked Windows root coverage, permission-first assertions, import sentinels and ownership /
  OpenAPI assertions.
- Regenerated Route manifests and the compatibility OpenAPI snapshot. Ownership is now `286 new /
  57 legacy` route decorators and `306 new / 61 legacy` method bindings; OpenAPI is `264 paths /
  314 operations`.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `600 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Local-directory application scope -> `9 passed`; app-factory/local-settings plus required P5/P8
  regressions -> `51 passed, 1` existing Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 Identity and Database Maintenance regression scope -> `23 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-p115-local-directories-20260829/`.

Focused review:

- Reviewed POSIX root/parent handling, Windows drive enumeration, hidden/file filtering,
  permission-first behavior, route precedence, fixture provenance, provider/import boundaries and
  ownership/OpenAPI uniqueness. The only material coverage gap was the Windows branch; a mocked
  drive-bitmask regression was added and passed. No P0/P1/P2 remains in this slice.

Stage full gate:

- Not run. P11 still has explicit route/config/table/Worker execution and compatibility-runtime
  gaps; this checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 27 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the next evidence-backed residual route/config/table or processed Worker execution
slice. Keep every unmigrated task/binding explicit and Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-MoviePilot-Settings-Ownership

Stage / Batch: P11 / native MoviePilot and subscription-strategy settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `965884f7` (`refactor(p11): migrate local directory browsing`).
- Checkpoint: this batch's `refactor(p11): assign moviepilot settings ownership` commit after the
  targeted gate below.

Implemented:

- Assigned `mp_config` to `MoviePilotConfigService` semantics and
  `SqlAlchemyAppSettingsRepository` persistence. Replacement removes retired/stale keys and stores
  one canonical JSONB value; the key is now explicitly classified as secret-bearing.
- Assigned `subscription_strategy_config` to `SubscriptionStrategyService` semantics and the same
  repository. Source filtering and the conditional MoviePilot-assistant disable commit through one
  `save_many()` UoW when both rows must change.
- Kept `LegacyAssistantDefaultsAdapter`, `watchlist_config` fallback, availability/provider probes,
  MoviePilot execution adapters and legacy consumers explicit. Settings ownership does not claim
  native MoviePilot task/external execution ownership.
- Standalone settings ownership is now `28 new / 25 legacy`; route, table, dynamic/bootstrap config,
  Worker and frontend counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `600 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- MoviePilot/strategy, subscription-core and required P5/P8 application regressions -> `43 passed`.
- Disposable PostgreSQL 18 MoviePilot settings, Identity and Database Maintenance scope ->
  `27 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-moviepilot-settings-20260829/`.

Focused review:

- Confirmed the two contract IDs, service/repository split, conditional `save_many()` transaction,
  rollback behavior, route fixture provenance and compatibility boundaries. Review found P2 gaps
  in raw replacement proof, existing-row rollback and overly broad notes; direct repository
  readback, an `ON CONFLICT DO UPDATE` rollback case and precise fallback/conditional notes were
  added. No P0/P1/P2 remains in this slice.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 25 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: remove the legacy default provider for the native standalone settings service, or continue an
equally evidence-backed residual setting/route slice. Keep Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Standalone-Plaintext-Defaults

Stage / Batch: P11 / plaintext standalone defaults and settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `1112f751` (`refactor(p11): assign moviepilot settings ownership`).
- Checkpoint: this batch's `refactor(p11): migrate standalone settings defaults` commit after the
  targeted gate below.

Implemented:

- Moved the exact six Telegram notification templates and nine AI prompts from legacy
  `constants.py` / `utils.py` into side-effect-free `etk.policy_defaults`. The legacy modules now
  re-export the same objects, preserving object identity and existing callers.
- Added `PlaintextStandaloneSettingsDefaults` with copy isolation and changed native app-factory
  wiring to use it directly. `LegacyStandaloneSettingsDefaults` remains only as a compatibility
  subclass and no longer imports `constants` or `utils`.
- Assigned `ai_user_prompts` and `telegram_notification_templates` to
  `StandaloneSettingsService` semantics and `SqlAlchemyAppSettingsRepository` persistence. Legacy
  routes and consumers remain explicit until controlled removal.
- Standalone settings ownership is now `30 new / 23 legacy`; route, table, dynamic/bootstrap config,
  Worker and frontend counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `600 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- App factory, standalone settings, exact policy defaults and required P5/P8 application
  regressions -> `55 passed, 1` existing Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 standalone settings, Identity and Database Maintenance scope ->
  `24 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-standalone-defaults-20260829/`.

Focused review:

- Verified exact moved literals with frozen SHA-256
  `37be319fab42af1b94539b70a1d0f9414dbd0fe99d0679c89d6a255bdf985951`, legacy object identity,
  copy isolation, native wiring, import direction, setting IDs and PostgreSQL snapshot/restore.
- Review found P2 gaps in post-reset default proof, controlled-removal notes and accidental legacy
  formatter churn. Post-reset default assertions and explicit legacy-consumer notes were added;
  `constants.py` / `utils.py` were reconstructed from HEAD with only the intended literal relocation
  and re-export changes. No P0/P1/P2 remains in this slice.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 23 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue an evidence-backed standalone setting, residual route or normal processed Worker
slice. Keep Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Cover-Settings-Ownership

Stage / Batch: P11 / native cover-generator settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `e47a0d6f` (`refactor(p11): migrate standalone settings defaults`).
- Checkpoint: this batch's `refactor(p11): assign cover settings ownership` commit after the
  targeted gate below.

Implemented:

- Assigned `cover_generator_config` to `CollectionService` default-merge/replacement semantics and
  `SqlAlchemyAppSettingsRepository` persistence. Cover rendering, provider reads and effect
  execution remain separate explicit boundaries.
- Added cover-specific permission-first, default merge, opaque payload, non-object pass-through,
  unsupported content type, malformed JSON and repository-failure HTTP evidence.
- Repaired two legacy-compatibility gaps found by review: authorized malformed JSON now preserves
  the legacy JSON `500` envelope, and a truthy corrupted non-object stored value returns the legacy
  read failure while an empty value still falls back to defaults.
- `COVER_DEFAULTS` and stored mappings are deep-copied on read so a service caller cannot mutate the
  process-global default lists. PostgreSQL 18 evidence now covers raw JSONB replacement, failed
  update rollback and exact snapshot/timestamp restoration.
- Standalone settings ownership is now `31 new / 22 legacy`; route, table, dynamic/bootstrap config,
  Worker and frontend counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `601 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- App factory, cover/collection and required P5/P8 application regressions -> `41 passed, 1`
  existing Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 cover settings, Identity and Database Maintenance scope -> `24 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-cover-settings-20260829/`.

Focused review:

- Confirmed the setting ID, service/repository boundary, permission/body order, fixture and
  ownership metadata, raw JSONB replacement, rollback, cleanup and provider/effect isolation.
- Review found one P1 malformed-JSON drift and P2 corrupted-value, non-object pass-through and
  mutable-default gaps. The route/service and tests were repaired as described above; no P0/P1/P2
  remains in this slice.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 22 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue canonical actor/watchlist/cleanup settings with explicit fallback ownership, or an
equally evidence-backed residual route/Worker slice. Keep Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Washing-Parity-Fix

Stage / Batch: P11 / post-checkpoint washing evaluator parity fix

HEAD / Commit:

- Pre-fix HEAD: `10a57a33` (`refactor(p11): assign cover settings ownership`).
- Checkpoint: this batch's `fix(p11): preserve washing evaluator parity` commit after the targeted
  gate below.

Implemented:

- Restored the legacy truthy fallback order for video width, height and size. A stream/source value
  of numeric zero no longer blocks the next valid MediaSource or injected file-size candidate.
- Added the legacy fixed `台灣 -> chi` alias while retaining the native `臺灣` compatibility alias.
- Added a focused regression using a zero-valued video stream and MediaSource size with valid
  fallback dimensions/bytes plus custom-empty language mapping evidence.

Targeted gate:

- Scoped Ruff check and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- Washing recalculation application scope -> `39 passed`.
- Disposable PostgreSQL 18 washing recalculation regression -> `1 passed`.
- `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-washing-parity-fix-20260829/`.

Focused review:

- A read-only legacy/native audit found no P0/P1 and two P2 behavior drifts: numeric-zero fallback
  and the fixed traditional-Chinese Taiwan alias. Both were repaired with the minimal evaluator
  changes above; the rerun is clean. Broader differential and multi-version PostgreSQL coverage
  remain validation-strength opportunities, not evidence of another current behavior defect.

Stage full gate:

- Not run. This is a bounded post-checkpoint compatibility fix; P11 ownership, native Worker,
  controlled removal and NFR/security gates remain pending.

Next: continue the canonical Actor/Watchlist/Cleanup settings ownership slice, keeping shared and
cleanup legacy fallback keys and cleanup execution readers explicit. Keep Live Acceptance
`HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Subscription-Settings-Ownership

Stage / Batch: P11 / canonical subscription settings ownership slice

HEAD / Commit:

- Pre-slice HEAD: `90a06baa` (`fix(p11): preserve washing evaluator parity`).
- Checkpoint: this batch's `refactor(p11): assign subscription settings ownership` commit after the
  targeted gate below.

Implemented:

- Assigned `actor_subscriptions_default_config`, `watchlist_config` and `media_cleanup_config` to
  their native Actor/Watchlist/Cleanup service semantics and
  `SqlAlchemyAppSettingsRepository` JSONB persistence.
- Preserved Actor opaque replacement and create-time default fallback, Watchlist's canonical
  migration from `shared_resource_config`, and Cleanup's whole-canonical-object precedence and
  exact rule normalization.
- Kept `shared_resource_config` plus the four legacy Cleanup keys explicitly legacy-owned. Cleanup
  task/maintenance readers retain their field-level fallbacks until the later cleanup execution
  migration; this settings slice does not claim Worker execution ownership.
- Added per-domain manifest assertions and one exact PostgreSQL 18 snapshot/restore test covering
  canonical raw JSONB, Actor create fallback, Watchlist migration without legacy mutation, Cleanup
  canonical precedence, retained legacy rows and failed-update rollback.
- Standalone settings ownership is now `34 new / 19 legacy`; route, table, dynamic/bootstrap config,
  Worker and frontend counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `602 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Actor/Watchlist/Cleanup plus required P5/P8 application regressions -> `84 passed`.
- Disposable PostgreSQL 18 subscription settings, Identity and Database Maintenance scope ->
  `24 passed`.
- Architecture -> `73 passed, 1` existing Starlette WSGI deprecation warning.
- Manifest/OpenAPI checks, fixture secret scan and `git diff --check` -> clean.
- Post-review scoped Ruff/Pyright and PostgreSQL 18 rules-precedence rerun -> clean, `1 passed`.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-subscription-settings-20260829/`.

Focused review:

- Confirmed all three setting IDs, service/repository wiring, Actor opaque payload behavior,
  Watchlist fallback mutation boundary, Cleanup canonical/legacy precedence, raw replacement,
  rollback and exact timestamp restoration. The initial review's Cleanup execution ambiguity and
  PostgreSQL rules-precedence gap were closed by explicit ownership notes and direct assertions.
  No P0/P1/P2 remains in this settings slice.
- Cleanup legacy Worker/maintenance field-level fallback remains a conditional P1 for the future
  cleanup execution migration and cannot be treated as resolved by settings ownership alone.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 19 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker until their normal processed paths have native owners.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue an evidence-backed residual route, standalone setting or normal processed Worker
slice. Keep cleanup execution fallbacks explicit and Live Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Credential-Ownership

Stage / Batch: P11 / native account-bound P115 credential ownership slice

HEAD / Commit:

- Pre-slice HEAD: `364690ab` (`refactor(p11): assign subscription settings ownership`).
- Checkpoint: this batch's `refactor(p11): migrate p115 credential ownership` commit after the
  targeted gate below.

Implemented:

- Added `SqlAlchemyP115CredentialRepository` as the `p115_auth_tokens` PostgreSQL owner. It preserves
  stable-account Token/Cookie replacement, access/refresh CAS, same-account coexistence, no-op
  timestamps, malformed truthy JSON fail-closed behavior and exact rollback.
- Unified native and legacy writers on the existing P115 advisory-lock identity before row locking.
  A PostgreSQL 18 race proves that a native writer blocks behind a legacy session lock, rereads the
  newer row and refuses a stale CAS instead of overwriting it.
- API startup installs a lazy native compatibility gateway; compatibility and native-only Workers
  install the repository directly and clear it on failure/close. Active legacy provider reads and
  refresh writes delegate through this owner while the no-gateway fallback remains explicit until
  controlled removal.
- Invalidated OpenAPI, token and Cookie/app-type caches only after committed credential changes.
  Restored legacy OpenAPI QR parity: an access-only success clears the QR session but does not
  overwrite the stored refreshable credential.
- Regenerated config ownership. Standalone settings are now `35 new / 18 legacy`; Route, Task,
  Table, dynamic/bootstrap config, frontend and native-only Worker counts are unchanged.

Targeted gate:

- Whole-scope Ruff -> clean; format check -> `606 files already formatted`; Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- App factory, Worker lifecycle, compatibility bridge and P115 auth/directory/fingerprint/
  MediaInfo/organize/STRM application scope -> `231 passed, 1` existing Starlette WSGI warning.
- P115 credential and dependent architecture scope -> `22 passed`.
- Disposable PostgreSQL 18 credential, legacy persistence, directory-cache, organize-plan and
  MediaInfo scope -> `35 passed`.
- Manifest check, fixture secret scan and `git diff --check` -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-p115-credentials-20260829/`.

Focused review:

- Initial review found one P1 cross-writer lock race and P2 cache, access-only QR and malformed-JSON
  parity gaps. The shared advisory lock, committed-change callback and explicit parity checks close
  all four findings. Re-review found no remaining P0/P1/P2 in this slice.

Stage full gate:

- Not run. P11 still has route/config/table/Worker execution and compatibility-runtime gaps; this
  checkpoint does not authorize controlled removal or P12.

Pending:

- Sixty-one method bindings, 57 route decorators, 12 tables, 82 dynamic config keys, seven
  bootstrap keys and 18 standalone settings remain legacy-owned. Thirty-nine catalog tasks remain
  unavailable in the native-only Worker.
- `subscribe-assistant-maintenance` has a declared native service/manifest owner but is still blocked
  by the native-only Worker precheck; its actual dependency graph must be verified before allowlist
  promotion. Controlled removal and all P11 NFR/security/SBOM/License gates remain pending.

Next: close the `subscribe-assistant-maintenance` manifest/runtime inconsistency only if its normal
processed path is fully native; otherwise correct the ownership claim. Keep Live Acceptance
`HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Assistant-Execution-Ownership-Truth

Stage / Batch: P11 / Subscribe Assistant execution-ownership correction

HEAD / Commit:

- Pre-slice HEAD: `fbac80f3` (`refactor(p11): migrate p115 credential ownership`).
- Checkpoint: this batch's `fix(p11): report assistant compatibility execution` commit after the
  targeted gate below.

Implemented:

- Corrected `subscribe-assistant-maintenance` from a false `native-use-case` execution claim to an
  explicit native envelope plus `LegacySubscribeAssistantMaintenanceGateway` processed-execution
  boundary. Its eight configurable phases still use the legacy Subscribe Assistant manager/store,
  so the task remains outside the native-only executable set.
- Added a generator-level compatibility-owner mapping and an architecture regression that freezes
  the envelope owner, real gateway owner, retirement stage and compatibility validation method.
- Corrected the Redis-Celery restart fixture to start a compatibility-enabled Worker explicitly.
  The prior default native-only run now fails honestly with `NativeTaskExecutionUnavailable`, as
  already required by the exhaustive Worker negative test.

Targeted gate:

- Scoped Ruff -> clean; format check -> `3 files already formatted`; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Subscribe Assistant, ownership and Worker outcome application/architecture scope -> `86 passed`.
- Disposable PostgreSQL 18 Subscribe Assistant runtime scope -> `12 passed`.
- Redis-Celery compatibility restart/resume path -> `1 passed, 10 deselected` after the initial
  default-native-only failure exposed the stale fixture assumption.
- Manifest currentness and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-assistant-ownership-20260829/`.

Focused review:

- Initial review found one P1: the first correction named the generic P7 compatibility gateway
  instead of the real assistant-specific gateway. The generator, manifest and assertion now use
  `LegacySubscribeAssistantMaintenanceGateway`; rereview found no remaining P0/P1/P2 and no other
  task was changed.

Stage full gate:

- Not run. This slice makes execution ownership truthful; it does not migrate the eight assistant
  phases, authorize controlled removal or start P12.

Pending:

- Native-only Worker execution remains `11 executable / 39 unavailable`. Route, table,
  dynamic/bootstrap config and standalone-setting counts are unchanged.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: add direct regression evidence for the newly audited washing-evaluator edge coverage, then
continue the smallest normal processed Worker candidate (`scan_old_seasons_backfill`) without
promoting any task before its processor-free repository/service path passes.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Washing-Parity-Edge-Evidence

Stage / Batch: P11 / supplemental washing-evaluator parity evidence

HEAD / Commit:

- Pre-slice HEAD: `07d38fe4` (`fix(p11): report assistant compatibility execution`).
- Checkpoint: this batch's `test(p11): lock washing parity edges` commit after the targeted gate
  below.

Implemented:

- Added direct regressions for legacy normal-rule truthiness: a list containing an empty rule is a
  normal rule, while truthy `is_exclude` is not; no scalar-rule parity claim was added.
- Locked the no-filename normalized source as Chinese `未知` through the full
  `normalize_washing_info()` path.
- Exercised a real `SqlAlchemyP115WashingRecalculationRepository.load_item()` projection with an
  invalid cache size. Conversion fails at row load, the service counts one row error, performs no
  persistence and does not acknowledge the pending revision.

Targeted gate:

- Scoped Ruff -> clean; format check -> `1 file already formatted`; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Washing recalculation/priority application scope -> `47 passed`.
- Disposable PostgreSQL 18 washing recalculation/priority scope -> `2 passed`.
- `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-washing-parity-edges-20260829/`.

Focused review:

- Initial review found one P2 because the invalid-size test separately exercised a private helper
  and a fake repository exception. It now traverses the real SQLAlchemy repository mapping before
  the service row-error boundary; rereview found no remaining P0/P1/P2.

Stage full gate:

- Not run. This test-only evidence slice does not change ownership counts, authorize controlled
  removal or start P12.

Pending:

- Native-only Worker execution remains `11 executable / 39 unavailable`; all residual route,
  table, config and compatibility-runtime work remains explicit.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: migrate `scan_old_seasons_backfill` only after its normal database mutation and request-stage
path has a processor-free SQLAlchemy service, progress/cancellation and import-sentinel evidence.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Old-Season-Backfill-Worker

Stage / Batch: P11 / processor-free old-season backfill Worker slice

HEAD / Commit:

- Pre-slice HEAD: `5c146ce7` (`test(p11): lock washing parity edges`).
- Checkpoint: this batch's `refactor(p11): execute native old-season backfill` commit after the
  targeted gate below.

Implemented:

- Replaced the normal processed `scan_old_seasons_backfill` compatibility call with
  `OldSeasonBackfillService` and `SqlAlchemyWatchlistMediaProjectionRepository`. Both native-only
  and compatibility-enabled Workers inject the processor-free service; neither resolves a
  Watchlist Processor for this task.
- Preserved the full-library, empty-payload task contract and the deterministic
  `scan_old_seasons_backfill` / `backfill_old_season` source. Candidate and affected counts remain
  distinct, and the persisted Worker result reports `compatibility_fallback=false`.
- Restricted parent activation to `media_metadata.item_type='Series'`, preventing an active Movie
  with the same TMDb ID from activating an inactive Series' old seasons.
- Kept each write batch in one PostgreSQL UoW with cancellation checkpoints every 50 items and
  immediately before commit. Source replay is atomic and idempotent; cancellation or an exception
  after the first write rolls back the complete batch.
- Promoted only this task's execution owner to
  `etk.domains.subscription.watchlist.OldSeasonBackfillService`; all other unavailable task owners
  remain explicit.

Targeted gate:

- Scoped Ruff -> clean; format check -> `11 files already formatted`; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Application/Worker/architecture scope -> `117 passed, 1` existing Starlette WSGI deprecation
  warning.
- Disposable PostgreSQL 18 watchlist projection scope -> `13 passed`, including Series parent
  identity, two-session same-source convergence, cancellation rollback and mid-batch write-error
  rollback.
- Redis-Celery native-only and compatibility-enabled profiles -> `2 passed, 11 deselected`; both
  persisted the expected candidate/affected counts and native execution details, and duplicate
  delivery remained suppressed.
- Task manifest generation reported `task_keys=50`; manifest currentness and `git diff --check`
  were clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-old-season-backfill-20260829/`.

Focused review:

- Initial review found one P1 (parent join not restricted to Series) and three P2 evidence gaps
  (concurrent same-source write, mid-batch exception rollback and persisted Worker result). A final
  profile audit added compatibility-enabled Redis-Celery evidence and clarified the manifest note's
  service boundary.
- Final findings-first rereview found no remaining P0/P1/P2. The contract is intentionally a
  non-locking candidate scan followed by an atomic idempotent write batch; it does not claim
  exactly-once scanning or a cross-UoW snapshot.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone
  settings ownership gaps, so controlled removal and the stage NFR/security gates are not yet
  authorized.

Pending:

- Native-only Worker execution is now `12 executable / 38 unavailable`. Route, table, config and
  standalone-settings counts are otherwise unchanged.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the smallest evidence-backed P11 pre-removal ownership slice. Do not begin controlled
removal or P12 until all route/task/table/config/frontend ownership reaches 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Quota-Projection-Replay-Fix

Stage / Batch: P11 / subscription quota compatibility-projection correctness fix

HEAD / Commit:

- Pre-slice HEAD: `9c08742a` (`refactor(p11): execute native old-season backfill`).
- Checkpoint: this batch's `fix(p11): refresh quota projection on replay` commit after the targeted
  gate below.

Implemented:

- Fixed `_reserve_quota_in_uow()` so an idempotent reservation replay recomputes ledger availability
  and refreshes `subscription_quota_state` in the same UoW after other expired reservations are
  released. The replay still returns the original reservation and creates no duplicate quota row.
- Added a PostgreSQL 18 regression with one expired reservation and one replayed retained
  reservation; the ledger states and projected remaining quota must converge atomically.
- Kept `subscription_quota_state` explicitly legacy-owned. Active compatibility tasks still read or
  directly write that row, so this correctness fix does not falsely promote the projection to a
  native source of truth.

Targeted gate:

- Scoped Ruff -> clean; format check -> `2 files already formatted`; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- Subscription/resubscribe application regressions -> `10 passed`.
- Disposable PostgreSQL 18 subscription runtime scope -> `19 passed`.
- `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-quota-projection-replay-20260829/`.

Focused review:

- The initial ownership audit found one P2: releasing an expired reservation before returning an
  existing replay could leave the compatibility projection stale. The manifest promotion draft was
  withdrawn because direct compatibility readers/writers remain active.
- Final findings-first rereview found no remaining P0/P1/P2 in this two-file fix.

Stage full gate:

- Not run. This post-checkpoint correctness fix changes no P11 ownership counts and does not
  authorize controlled removal or P12.

Pending:

- Native-only Worker execution remains `12 executable / 38 unavailable`; route, table,
  dynamic/bootstrap config and standalone-setting counts are unchanged.
- `subscription_quota_state` can be promoted only after its active compatibility readers/writers
  are migrated or routed through the native quota ledger.

Next: continue with the smallest processor-free native Worker candidate while extracting any
runtime provider adapter that still imports legacy modules. Do not equate a typed service with a
fully removable execution path.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Directory-Sync-Worker

Stage / Batch: P11 / processor-free P115 directory-sync Worker slice

HEAD / Commit:

- Pre-slice HEAD: `16b9957d` (`fix(p11): refresh quota projection on replay`).
- Checkpoint: this batch's `refactor(p11): execute native p115 directory sync` commit after the
  targeted gate below.

Implemented:

- Replaced the compatibility directory provider in both Worker profiles with an account-bound
  `HttpxP115DirectoryTreeGateway`; native-only execution now wires the real
  `P115DirectorySyncService` and SQLAlchemy cache repository without resolving a Processor or
  importing legacy `handler/routes/database/tasks/task_manager/extensions` modules.
- Added OpenAPI/Cookie request contracts, configured backend preference with first-page fallback
  and backend pinning, finite transport retries, legacy list/field normalization and authoritative
  empty-page fencing. The transport never refreshes or writes credentials.
- Added request-before/after account, credential and app-type fencing. Credential drift is a
  dedicated fail-closed error that cannot be swallowed by backend fallback, including HTTP failure
  paths; explicit provider failure cannot be overwritten by `errno=0` or `code=200`.
- Malformed directory aliases now abort that root instead of reconciling a partial set. PostgreSQL
  18 failures for HTTP 401/503, contradictory provider state and malformed raw aliases preserve
  both credentials and stale cache rows.
- Added native-only and compatibility-enabled Redis/Celery execution through a loopback HTTP Stub.
  The native-only profile kills the Worker after cache commit but before Job result persistence;
  a replacement Worker replays through attempt 2, converges to one cache row, persists the result,
  and leaves the retry dispatch Outbox processed.
- Hardened the P4 integration harness so setup failures still close database/Redis/process/HTTP
  resources and restore pre-existing P115 settings exactly. Native-only Worker execution is now
  `13 executable / 37 unavailable`.

Targeted gate:

- Scoped Ruff -> clean; format check -> `13 files already formatted`; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`.
- P115 auth/directory, real Worker, process-role and architecture scope -> `232 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 directory-cache/credential scope -> `16 passed`.
- Redis-Celery native-only crash/replay and compatibility-enabled normal profile -> `2 passed, 13
  deselected`; only loopback Stub HTTP was used.
- Manifest generation/check reported `task_keys=50`; OpenAPI currentness and `git diff --check`
  were clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-p115-directory-sync-20260829/`.

Focused review:

- Initial reviews found two transport P1s (credential drift swallowed by fallback and explicit
  provider failure overwritten by compatibility success fields), failure-path PG18 gaps, malformed
  alias cleanup risk, incomplete Redis fixture cleanup and missing P115-specific Worker replay
  evidence. All were repaired with focused regressions.
- Final findings-first rereview found no remaining P0/P1/P2 in this slice. The replay recovery
  Coordinator is explicitly compatibility-enabled; migrating native-only Coordinator recovery is
  still part of the later controlled-removal boundary, not claimed by this Worker checkpoint.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone
  settings ownership gaps, so controlled removal and the stage NFR/security gates are not yet
  authorized.

Pending:

- Native-only Worker execution is `13 executable / 37 unavailable`. Fifty-seven route decorators,
  61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18 standalone
  settings remain legacy-owned.
- `repair-p115-fingerprints` is explicitly NO-GO until its native HTTP provider, local-path/SHA1
  parity, complete credential fencing and processed execution evidence are closed. Controlled
  removal, security/performance/reliability/resource gates, SBOM, vulnerability report, License
  inventory and P12 remain pending.

Next: select the smallest evidence-backed remaining normal processed Worker/ownership slice; do not
promote `repair-p115-fingerprints` by allowlist alone and do not start controlled removal or P12.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Washing-Processor-Free-Fix

Stage / Batch: P11 / compatibility Worker processor-boundary fix

HEAD / Commit:

- Pre-fix HEAD: `19cdc512` (`refactor(p11): execute native p115 directory sync`).
- Checkpoint: this batch's `fix(p11): keep washing recalculation processor-free` commit after the
  targeted gate below.

Implemented:

- Added `recalculate_library_washing_priorities` to the compatibility Worker's processor-free
  execution set. Both Worker profiles now enter the already-native
  `P115WashingPriorityRecalculationService` without resolving `extensions.media_processor_instance`.
- Kept the evaluator, repository, task payload and ownership unchanged; current review found no
  new evaluator/repository P0/P1/P2.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- Worker outcome plus washing recalculation application scope -> `114 passed`.
- A separate read-only evaluator audit ran the washing priority/recalculation scope -> `47 passed`.
- `git diff --check` -> clean. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-washing-processor-free-fix-20260829/`.

Stage full gate:

- Not run. This is a bounded P2 process-boundary fix; the P11 ownership, controlled-removal and NFR
  gates remain pending.

Next: execute the processor-free `scan-cleanup-issues` slice with native user-scope projection,
fail-closed Emby readback, atomic cleanup-index replacement and Worker replay evidence. Keep Live
Acceptance `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Cleanup-Scan-Worker

Stage / Batch: P11 / processor-free cleanup-scan Worker slice

HEAD / Commit:

- Pre-slice HEAD: `48f8b566` (`fix(p11): keep washing recalculation processor-free`).
- Checkpoint: this batch's `refactor(p11): execute native cleanup scan` commit after the targeted
  gate below.

Implemented:

- Replaced the normal processed `scan-cleanup-issues` compatibility call with
  `CleanupScanService`, `SqlAlchemyCleanupScanRepository`, the P5-owned cleanup media projection
  and a read-only `HttpxCleanupEmbyInventoryGateway`. Native-only and compatibility-enabled Workers
  use the same processor-free service and propagate the requesting user and durable Job identity.
- Preserved legacy version ordering, Chinese-subtitle bypass, per-resolution retention and
  canonical/legacy cleanup-policy fallback. Malformed canonical config or user permission arrays
  now fail closed before Emby inventory or publication.
- Added user-visible Movie/Series scope, Movie/Episode version projection and authoritative batched
  Emby identity readback. Transport, invalid payload/object/identity and credential drift all abort
  without replacing the pending index.
- Replaced pending cleanup rows in one PostgreSQL UoW while preserving ignored rows, rejecting active
  cleanup execution, revalidating the source fingerprint and fencing current/parent cancellation.
  Source tables remain locked through commit; commit-time cancellation rolls back, while effect
  markers prevent a committed publication from being reported as cancelled.
- Standardized Database Maintenance rebuild ordering to `media_metadata -> cleanup_index`, matching
  cleanup scan and offline cleanup. This removes the two previously opposing lock orders without
  changing the final single-UoW rebuild result.
- Promoted only `scan-cleanup-issues` to the native-only executable set. Execution is now `14
  executable / 36 unavailable`; every other unmigrated task remains explicit.

Targeted gate:

- Scoped Ruff -> clean; format check -> `15 files already formatted`; scoped Pyright -> `0 errors,
  0 warnings, 0 informations`.
- Application/Worker/architecture scope -> `146 passed, 1` existing Starlette WSGI deprecation
  warning.
- Disposable PostgreSQL 18 cleanup scan plus Database Maintenance -> `15 passed`, including source
  writer locks, cancellation rollback/linearization, malformed policy preservation and the
  maintenance cross-lock ordering regression.
- Existing disposable PostgreSQL 18 cleanup execution regression -> `46 passed`.
- Redis-Celery native-only crash/replacement replay and compatibility-enabled normal profile -> `2
  passed, 15 deselected`; only a loopback Emby Stub was used.
- Task export reported `task_keys=50`; Manifest and OpenAPI currentness plus `git diff --check` were
  clean. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-cleanup-scan-20260829/targeted-gate-final.md`.

Focused review:

- Initial reviews found source-fingerprint publication drift, malformed-policy widening,
  cancellation publication, and two opposing Database Maintenance lock orders. A suspected
  `claim_next()` race was withdrawn because claim cannot create an active item outside the exact
  state set already checked by scan.
- Final independent rereviews found no remaining P0/P1/P2 after source-first locking, Job/effect
  fencing, fail-closed validation and the maintenance lock-order correction.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone
  settings ownership gaps, so controlled removal and the stage NFR/security gates are not yet
  authorized.

Pending:

- Native-only Worker execution is `14 executable / 36 unavailable`. Fifty-seven route decorators,
  61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18 standalone
  settings remain legacy-owned.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the smallest evidence-backed P11 ownership or normal processed Worker slice. Do not
start controlled removal or P12 until execution and ownership reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Cleanup-Config-Fail-Closed

Stage / Batch: P11 / cleanup canonical-config correctness fix

HEAD / Commit:

- Pre-fix HEAD: `ec898a1e` (`refactor(p11): execute native cleanup scan`).
- Checkpoint: this batch's `fix(p11): reject malformed cleanup config` commit after the targeted
  gate below.

Implemented:

- Changed native `CleanupService.get_settings()` to treat every non-`None`
  `media_cleanup_config` value as authoritative. Falsy non-object JSON values now fail closed
  instead of silently widening to the four legacy fallback rows.
- Added HTTP regressions for `[]`, `""`, `0` and `false`, matching the cleanup-scan policy boundary.
- A fresh ownership audit found that the four legacy fallback keys still have active readers in
  legacy cleanup execution and maintenance paths. Their proposed Manifest promotion was therefore
  rejected; ownership remains `35 new / 18 legacy` until those readers are migrated.

Targeted gate:

- Cleanup HTTP -> `22 passed`; cleanup-scan application regression -> `18 passed`.
- Disposable PostgreSQL 18 subscription-settings scope -> `1 passed`.
- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-cleanup-config-fail-closed-20260829/targeted-gate.md`.

Focused review:

- No remaining P0/P1/P2 in this two-file fix. The stale audit against `d8128be7` predates the
  native washing evaluator implementation and later parity checkpoints, so it does not justify
  another washing rewrite on the current HEAD.

Stage full gate:

- Not run. This correctness fix changes no ownership or executable-task counts and does not
  authorize controlled removal or P12.

Pending:

- Native-only Worker execution remains `14 executable / 36 unavailable`; route, table, dynamic and
  bootstrap config, standalone-setting and active compatibility-runtime gaps remain explicit.
- Cleanup execution still uses legacy media/P115 gateways; the four cleanup fallback keys cannot
  be promoted until those active readers are migrated.

Next: migrate the smallest processor-free normal task path. The current read-only task audit selects
`manual-correct-organize-records`, which already has a durable native reconciliation service and only
needs an account-bound read-only 115 provider plus Worker/replay evidence.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Organize-Reconciliation

Stage / Batch: P11 / processor-free organize-reconciliation Worker slice

HEAD / Commit:

- Pre-slice HEAD: `fc223207` (`fix(p11): reject malformed cleanup config`).
- Checkpoint: this batch's `refactor(p11): execute native organize reconciliation` commit after the
  targeted gate below.

Implemented:

- Added an account-bound `HttpxP115OrganizeReadbackGateway` that opens only the execution's persisted
  `openapi` or `cookie` backend, never falls back, reuses the native credential fences and exposes only
  authoritative `list_children` behavior. Its mutation methods fail explicitly.
- Added complete-page, stable-count, duplicate-page/ID and malformed-item validation. Transport,
  credential/account drift, invalid payloads and incomplete pagination fail closed without completing
  reconciliation.
- Split `P6P115TaskRouter` execution and reconciliation dependencies. `scan-organize-115` retains the
  legacy mutation-capable provider in compatibility mode, while `manual-correct-organize-records` uses
  the processor-free readback service in both Worker profiles.
- Promoted only `manual-correct-organize-records` to native-only execution. The Worker graph is now
  `15 executable / 35 unavailable`.
- Added Redis/Celery evidence for a native-only crash after reconciliation commit but before P4 Job
  result persistence. Attempt 2 reuses the completed approved intent, performs no second HTTP readback,
  and leaves the global indeterminate effect unchanged; the compatibility-enabled normal path uses the
  same native service and loopback 115 Stub.

Targeted gate:

- Scoped Ruff -> clean; format check -> `12 files already formatted`; scoped Pyright -> `0 errors,
  0 warnings, 0 informations`.
- Application, Worker, router, HTTP/adapter and architecture scope -> `211 passed, 1` existing Starlette
  WSGI deprecation warning; dedicated readback matrix -> `10 passed`.
- Disposable PostgreSQL 18 organize execution/reconciliation -> `9 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only crash/replay and compatibility-enabled normal
  profiles -> `2 passed, 17 deselected`; requests were limited to the loopback OpenAPI Stub.
- Task export reported `task_keys=50`; Manifest and OpenAPI currentness were clean; contract tests ->
  `9 passed`; `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-organize-reconciliation-20260829/`.

Focused review:

- No P0/P1 remains. The compatibility constructor's deliberate fallback from an omitted dedicated
  reconciliation service to `organize_execution` remains a P2 architecture note; `reconcile()` itself
  is read-only, and both production Worker profiles explicitly inject the readback-only service.
- The shared directory normalizer can infer a file from an otherwise type-less `id` alias. There is no
  provider-contract evidence that this existing alias must be rejected; unknown authoritative payload
  combinations remain a non-blocking Live/provider-schema residual.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone-setting
  ownership gaps, so controlled removal and stage NFR/security gates remain unauthorized.

Pending:

- Native-only Worker execution is `15 executable / 35 unavailable`. Fifty-seven route decorators, 61
  method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18 standalone settings
  remain legacy-owned.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the smallest evidence-backed P11 ownership or normal processed Worker slice. Do not
start controlled removal or P12 until execution and ownership reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P8-Effect-Task-Ownership

Stage / Batch: P11 / existing processor-free P8 task ownership correction

HEAD / Commit:

- Pre-slice HEAD: `9449c53e` (`refactor(p11): execute native organize reconciliation`).
- Checkpoint: this batch's `refactor(p11): record native p8 effect task owners` commit after the
  targeted gate below.

Implemented:

- Corrected `sync-all-user-data` execution ownership from the compatibility gateway to the already
  active `UserSyncService`. Both Worker profiles already inject the same native adapter/repository
  graph and the native-only Worker already executes this key without a Processor.
- Corrected `system-auto-update` to its frozen native `SKIPPED` owner. The router does not call
  Docker, update, restart, the legacy updater or the compatibility gateway.
- Added generator-level owner mappings and an architecture regression for both contracts. No
  runtime path or allowlist changed; native-only execution remains `15 executable / 35 unavailable`.

Targeted gate:

- Scoped Ruff -> clean; format -> one new architecture file formatted; scoped Pyright -> `0 errors,
  0 warnings, 0 informations`.
- P8 router, Worker outcome/process-role and ownership scope -> `121 passed, 1` existing Starlette
  WSGI deprecation warning.
- Disposable PostgreSQL 18 user-sync concurrency/projection scope -> `7 passed`.
- Task export reported `task_keys=50`; Manifest currentness and contract tests -> clean / `8 passed`;
  fixture secret scan and `git diff --check` -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-effect-task-ownership-20260829/`.

Focused review:

- No P0/P1/P2. This slice changes only truthful ownership metadata and its regression; the normal
  processed runtime was already native and remains unchanged. Redis/Celery was not rerun because no
  Worker wiring, routing, persistence or replay behavior changed.

Pending:

- Thirty-five catalog tasks remain unavailable in the native-only Worker. Route, table,
  dynamic/bootstrap config, standalone-setting and active compatibility-runtime gaps remain.
- Next candidate is `repair-p115-fingerprints`; it must add a native account/credential-fenced 115
  read provider and pass application, PostgreSQL 18, Redis/Celery and import-sentinel gates before
  allowlist promotion.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Fingerprint-Repair

Stage / Batch: P11 / processor-free P115 fingerprint-repair Worker slice

HEAD / Commit:

- Pre-slice HEAD: `332502fd` (`refactor(p11): record native p8 effect task owners`).
- Checkpoint: this batch's `refactor(p11): execute native fingerprint repair` commit after the
  targeted gate below.

Implemented:

- Replaced `LegacyP115FingerprintRemoteGateway` in both Worker profiles with the account-bound,
  read-only `HttpxP115FingerprintRemoteGateway`. It uses local PickCode-to-FID decoding, fixed
  OpenAPI/Cookie file-info endpoints, finite transport retries, request-before/after account and
  credential fencing, and never refreshes or saves credentials.
- Normalized confirmed 115 not-found codes to a missing asset while keeping HTTP/provider/JSON
  failures task-level. Provider identity or field conflicts fail the current asset closed and allow
  the repair scan to continue.
- Preserved authoritative file, parent, PickCode, SHA1, name, category and size validation. Cookie
  responses retain top-level path/breadcrumb evidence; a missing parent may use an explicit
  same-account OpenAPI readback, while Cookie-only ambiguous identity remains a per-asset conflict.
- Remote repair now carries the shortest root-contained real-video local path from the inspected
  STRM hints. PostgreSQL cache and media-array updates retain the existing account/revision/slot
  fencing and one-UoW commit boundary.
- Promoted only `repair-p115-fingerprints` to native-only execution. The Worker graph is now
  `16 executable / 34 unavailable`; all other unproven task paths remain explicit.

Targeted gate:

- Scoped Ruff check and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- Fingerprint/auth/directory Worker application and architecture scope -> `233 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 P115 MediaInfo/fingerprint repository scope -> `15 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only OpenAPI crash/replay,
  compatibility-enabled OpenAPI and native-only Cookie profiles -> `3 passed, 19 deselected`; only
  loopback HTTP Stubs were used.
- Task export reported `task_keys=50`; Manifest and OpenAPI currentness were clean; contract scope
  -> `16 passed`; `uv lock --check`, three fixture secret scans and `git diff --check` were clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-fingerprint-repair-20260829/`.

Focused review:

- Initial reviews found four P1 gaps: 115 not-found classification, task-wide handling of
  asset-local identity conflicts, lost local-path hints and incomplete Cookie parent recovery.
  They also identified name/category aliases, self-parent and unknown-size fail-closed gaps.
- The final delta rereview confirmed top-level breadcrumb preservation, same-account OpenAPI parent
  fallback, Cookie-only fail-closed behavior and the real Cookie-only Worker profile. No P0/P1/P2
  remains in this slice; checkpoint verdict is GO.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone-setting
  ownership gaps, so controlled removal and stage NFR/security gates remain unauthorized.

Pending:

- Native-only Worker execution is `16 executable / 34 unavailable`. Fifty-seven route decorators,
  61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18 standalone
  settings remain legacy-owned.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the smallest evidence-backed P11 ownership or normal processed Worker slice. Do not
start controlled removal or P12 until execution and ownership reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-MediaInfo-Restore

Stage / Batch: P11 / processor-free MediaInfo restore Worker slice

HEAD / Commit:

- Pre-slice HEAD: `b9d345ff` (`refactor(p11): execute native fingerprint repair`).
- Checkpoint: this batch's `refactor(p11): execute native mediainfo restore` commit after the
  targeted gate below.

Implemented:

- Promoted only `restore_mediainfo` to native-only execution. Both Worker profiles now inject the
  processor-free `P115MediaInfoRestoreService`; native-only execution is `17 executable / 33
  unavailable` and every other unproven task remains explicit.
- Added account- and credential-fenced `P115SdkDirectUrlGateway`. It uses the locked SDK's low-level
  `download_url_info_open` and `download_url_app(app="os_windows")` paths, passes finite timeouts,
  disables OpenAPI refresh, never saves credentials, and avoids the high-level stable-PickCode path
  that can read or mutate an empty provider root.
- Native-only MediaInfo effects use only the Bridge delivery graph. Unsupported non-terminal effects
  fail before claim and preserve pending/retryable/claimed state; terminal projections remain
  idempotent. Worker shutdown now tracks active media-effect execution and waits before closing the
  database/runtime graph.
- Added actual locked-SDK fake-request evidence, P5 router fail-closed coverage, Worker startup/import
  sentinels, PostgreSQL effect/UoW state coverage, and loopback Redis/Celery crash after confirmed
  Bridge write but before Job result persistence. Attempt 2 reuses the confirmed effect and performs
  no second Bridge write.

Targeted gate:

- `uv lock --check` -> clean; scoped Ruff -> clean; format -> `13 files already formatted`; scoped
  Pyright -> `0 errors, 0 warnings, 0 informations`; scoped compileall -> clean.
- P5/P6 task, direct-URL, MediaInfo, effect, Worker and architecture scope -> `314 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 MediaInfo/effect/UoW scope -> `34 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only crash/replay and compatibility-enabled normal
  profiles -> `2 passed, 22 deselected`; all Emby reads and Bridge writes used one loopback Stub.
- Manifest currentness and OpenAPI currentness -> clean; contract tests -> `16 passed`; `uv lock
  --check`, three fixture secret scans and `git diff --check` -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-mediainfo-restore-20260829/`.

Focused review:

- Initial review found one P1: high-level `p115client.download_url*` helpers implicitly resolve a
  stable PickCode and may upload/delete when the provider root is empty. The low-level read-only SDK
  calls and actual fake-request regression close that path.
- Final rereview found no P0/P1. A provider-contract P2 remains: `P115DirectUrl` does not propagate a
  temporary Cookie/header required by a possible `f=3` URL. The legacy gateway had the same contract,
  no real provider payload proves it is reachable, and Live Acceptance remains `HUMAN_PENDING`.

Stage full gate:

- Not run. P11 still has explicit route, task, table, dynamic/bootstrap config and standalone-setting
  ownership gaps, so controlled removal and stage NFR/security gates remain unauthorized.

Pending:

- Native-only Worker execution is `17 executable / 33 unavailable`. Fifty-seven route decorators,
  61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18 standalone
  settings remain legacy-owned.
- The latest washing-recalculation parity audit must be reconciled against the current evaluator and
  repository before relying on its normal processed path; only confirmed current deltas may be fixed.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: reconcile the supplied washing evaluator/legacy parity evidence against the current native
implementation, then continue the smallest evidence-backed P11 execution or ownership slice. Do not
start controlled removal or P12 until execution and ownership reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Washing-Parity-Reconcile

Stage / Batch: P11 / washing recalculation parity reconciliation

HEAD / Commit:

- Pre-fix HEAD: `79b35311` (`refactor(p11): execute native mediainfo restore`).
- Checkpoint: this batch's `fix(p11): reconcile washing recalculation parity` commit after the
  targeted gate below.

Implemented:

- Preserved legacy handling for explicit MediaInfo `Language=None`: the stream value is normalized
  through the configured unknown-language alias and remains matchable as `und` instead of being
  discarded.
- Forced version-slot match rules to ordinary all-condition semantics even when a raw helper caller
  supplies truthy `is_exclude`, matching the legacy resolver. Persisted canonical config already
  strips this unknown field; the direct helper contract is now safe if that projection changes.
- Isolated organize-record target-CID backfill in a PostgreSQL savepoint. A backfill failure remains
  best effort while cache and media washing snapshots still commit in the same outer item UoW;
  source-fingerprint drift continues to fail before this boundary.
- Reconciled three supplied audit claims against the current legacy source. Both implementations
  intentionally filter empty SHA1/PickCode entries, scan later asset entries for a filename fallback,
  and select non-empty organize targets without a `status='success'` predicate, so no speculative
  change was made. The previously documented `臺灣 -> chi` native compatibility alias remains.

Targeted gate:

- Scoped Ruff check/format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  scoped compileall and `git diff --check` -> clean.
- Washing evaluator, priority, native Worker and P7 runtime application scope -> `69 passed`.
- Disposable PostgreSQL 18 washing recalculation/priority scope -> `2 passed`; the regression injects
  a real division-by-zero database error and proves the savepoint leaves the outer UoW committable.
- Manifest and OpenAPI currentness plus all three fixture/contract secret scans -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-washing-parity-reconcile-20260829/`.

Focused review:

- Initial read-only audits identified the unknown-language, raw slot-matching and backfill exception
  P2 deltas. The final delta review confirmed the savepoint does not swallow source drift or split the
  cache/media UoW. The slot regression was made explicitly discriminating between exclude-any and
  ordinary-all matching. No P0/P1/P2 remains in this slice.

Execution note:

- Two main-thread mechanical formatter commands were mistakenly dispatched concurrently against
  different files. They had no overlapping target or subagent writer; the complete tracked diff was
  audited immediately and every final gate above was rerun serially. All later writes remain serial.

Stage full gate:

- Not run. This bounded parity batch does not change Worker/route/table/config ownership counts and
  does not authorize controlled removal or P12.

Pending:

- Native-only Worker execution remains `17 executable / 33 unavailable`. Fifty-seven route
  decorators, 61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 18
  standalone settings remain legacy-owned.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue the smallest evidence-backed normal processed Worker or ownership slice. Do not start
controlled removal or P12 until execution and ownership reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Cleanup-Config-Cutover

Stage / Batch: P11 / cleanup canonical configuration cutover

HEAD / Commit:

- Pre-cutover HEAD: `21dcbcd5` (`fix(p11): reconcile washing recalculation parity`).
- Checkpoint: this batch's `refactor(p11): retire cleanup config fallbacks` commit after the
  targeted gate below.

Implemented:

- Added migration `0020_p11_cleanup_config_cutover`. In one PostgreSQL transaction it creates
  `media_cleanup_config` only when absent, copies the four former cleanup setting values, supplies
  typed defaults for missing rows, and leaves all historical source rows untouched. Existing
  canonical state always wins through `ON CONFLICT DO NOTHING`; downgrade is intentionally a
  non-destructive no-op, so the cutover is one-way.
- Added one canonical cleanup schema validator and routed native HTTP, cleanup scan, compatibility
  HTTP/task and maintenance readers through it. The canonical object is authoritative as a whole;
  malformed outer or nested fields fail closed, including missing/non-string rule IDs.
- Removed every operational read of `media_cleanup_rules`, `media_cleanup_library_ids`,
  `media_cleanup_keep_one_per_res` and `media_cleanup_delete_delay`. The Manifest exporter now keeps
  these historical contracts visible as `retired-approved` and fails generation if an operational
  reader returns.
- Regenerated Manifest/source inventory and advanced the single Alembic head to
  `0020_p11_cleanup_config_cutover`; OpenAPI behavior is unchanged.

Targeted gate:

- `uv lock --check`, scoped Ruff check/format, scoped Pyright, compileall and `git diff --check` ->
  clean.
- Cleanup HTTP/scan plus migration-head architecture scope -> `68 passed`; static Manifest/OpenAPI
  contract scope -> `9 passed`.
- Disposable PostgreSQL 18 cleanup/settings/database-head scope -> `19 passed`. Evidence includes
  real Alembic upgrade failure rollback, successful retry, one-way downgrade/re-upgrade, a blocked
  concurrent canonical writer, canonical precedence, malformed JSONB fail-closed behavior and the
  existing cleanup scan transaction/drift matrix.
- Manifest/OpenAPI currentness, Alembic single-head check, retired-key operational-reader sentinel
  and all three fixture/contract secret scans -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-cleanup-config-cutover-20260829/`.

Focused review:

- Initial review found two P1 issues: a PostgreSQL regression still required the removed fallback,
  and malformed migrated fields had inconsistent reader behavior. Both were corrected with the
  actual cutover path and shared validator. Follow-up P2 gaps for rule identity, real Alembic
  failure/downgrade and concurrent-writer evidence were also closed. Final review found no
  P0/P1/P2 in this slice.

Stage full gate:

- Not run. This cutover retires four standalone setting contracts but leaves task, route, table,
  dynamic/bootstrap config and other standalone ownership gaps, so controlled removal and P11
  stage NFR/security gates remain unauthorized.

Pending:

- Native-only Worker execution remains `17 executable / 33 unavailable`. Fifty-seven route
  decorators, 61 method bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 14
  standalone settings remain legacy-owned; four additional cleanup settings are now
  `retired-approved` with no operational reader.
- `intro-fingerprint-backfill` is not eligible for promotion until its favorite `force=True`,
  provider-backend pinning, P6/P9 shared-export ownership and historical favorite-source contracts
  are confirmed. No allowlist-only change was made; other independent P11 slices can continue.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue another evidence-backed P11 execution or ownership slice that does not depend on the
unresolved intro contracts. Do not start controlled removal or P12 until ownership reaches 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-License-Alias-Cutover

Stage / Batch: P11 / Pro license historical alias cutover

HEAD / Commit:

- Pre-cutover HEAD: `d67cbc2f` (`refactor(p11): retire cleanup config fallbacks`).
- Checkpoint: this batch's `refactor(p11): retire license key alias` commit after the targeted gate
  below.

Implemented:

- Added migration `0021_p11_license_alias_cutover`. It copies only a non-empty JSON string from the
  historical `license_key` row into canonical `pro_license_key` when the canonical row is absent,
  SQL/JSON null, empty or whitespace-only. Non-empty and malformed canonical values are never
  overwritten; the historical row remains for audit and downgrade is intentionally non-destructive.
- Removed the two operational storage fallbacks from shared-center quota reporting and the retained
  compatibility runtime tier hint. Pro activation/transfer still accepts the API request field named
  `license_key` and saves only canonical `pro_license_key` / `pro_expire_time`.
- Marked the storage alias `retired-approved`, added an AST sentinel for the excluded compatibility
  runtime source and regenerated Manifest/source-digest evidence. Standalone settings are now
  `35 new / 13 legacy / 5 retired-approved`; the single Alembic head is
  `0021_p11_license_alias_cutover`.

Targeted gate:

- `uv lock --check`, scoped Ruff check/format, scoped Pyright, compileall and `git diff --check` ->
  clean. The modified legacy shared-center module is covered by compile/application evidence rather
  than whole-file Ruff/Pyright because that preserved compatibility file remains outside the scoped
  native quality baseline and contains unrelated pre-existing findings.
- System/license/cleanup application plus migration-head architecture scope -> `63 passed`; static
  Manifest/OpenAPI contract scope -> `16 passed`.
- Disposable PostgreSQL 18 cross-stage scope -> `33 passed`; the post-review focused rerun ->
  `3 passed`. Evidence covers string-only alias acceptance, SQL/JSON null and whitespace handling,
  canonical precedence, preserved malformed canonical data, transaction rollback, real Alembic
  failure/retry, one-way downgrade/re-upgrade and a blocked concurrent canonical writer.
- Manifest/OpenAPI currentness, Alembic single-head check, retired-alias operational-reader sentinel
  and all three fixture/contract secret scans -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-license-alias-cutover-20260829/`.

Focused review:

- SQL cutover, concurrent `ON CONFLICT` precedence, rollback/downgrade, secret handling, storage/API
  name separation, Manifest sentinel and evidence quality were rereviewed. Final result: no
  P0/P1/P2 finding in this slice.

Stage full gate:

- Not run. This slice retires one historical storage alias but does not close the remaining task,
  route, table, dynamic/bootstrap config or standalone-setting ownership gaps; controlled removal
  and P11 NFR/security/SBOM/License gates remain unauthorized.

Pending:

- Native-only Worker execution remains `17 executable / 33 unavailable`; route, table,
  dynamic/bootstrap config and compatibility-runtime counts are unchanged from the previous slice.
- `intro-fingerprint-backfill` remains blocked on the four recorded favorite/provider/shared-export/
  historical-source contracts. Read-only audits also found that `scan-organize-115` needs decisions
  on empty-payload versus approved-plan execution, unrecognized-item side effects and target-CID
  binding; `update-daily-theme` needs explicit provider-failure, concurrent pool publication and
  trigger/result contracts. No allowlist-only promotion was made.
- Controlled removal, security/performance/reliability/resource gates, SBOM, vulnerability report,
  License inventory and P12 remain pending.

Next: continue another evidence-backed P11 execution or ownership slice that does not depend on the
recorded contract decisions. Do not start controlled removal or P12 until ownership and execution
reach 100%.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Contract-Decision-Hold

Stage / Batch: P11 / pre-removal contract-decision handoff

HEAD / Commit:

- Pre-record HEAD: `87fa5ccf` (`refactor(p11): retire license key alias`).
- This docs-only checkpoint records the next writer boundary; it does not authorize a runtime
  promotion, controlled removal or P12.

Implemented:

- Revalidated a clean worktree and current ownership truth: native-only Worker `17 executable / 33
  unavailable`; routes `286 / 343` decorators and `306 / 367` method bindings native-owned; tables
  `27 new / 12 legacy`; standalone settings `35 new / 13 legacy / 5 retired-approved`.
- Read-only task and ownership audits found no remaining decision-free semantic closure. A typed
  service, native envelope or compatibility projection is not sufficient evidence for native-only
  execution or ownership.
- Recorded five implementation-ready contract bundles that require explicit approval:
  `check-expired-users`, `intro-fingerprint-backfill`, `scan-organize-115`, `update-daily-theme`
  and the `subscription_quota_state` compatibility cutover.
- Reconciled the supplied washing audit provenance: its `d8128be7` baseline predates the native
  evaluator and later parity checkpoints. No current test or review failure justifies another
  washing rewrite.

Targeted gate:

- Read-only preflight: `git status --short --branch` -> clean;
  `git rev-parse HEAD` -> `87fa5ccf4ee684009116389fbd0e27973ed07782`;
  `git diff --check` -> clean.
- No application, PostgreSQL 18 or Redis/Celery gate was rerun because this checkpoint changes only
  Goal evidence and makes no runtime or contract mutation.

Stage full gate:

- Not run. P11 ownership/execution is incomplete and the controlled-removal/NFR wave remains
  unauthorized.

Pending:

- The recommended contract package preserves `expired` state with a per-user durable expiry
  operation, separates intro/shared effects, requires approved immutable organize executions,
  retains the last valid daily-theme pool on failure, and makes the quota reservation ledger the
  sole accounting fact. These are new P11 decisions and are not covered by the earlier P8 Database
  Maintenance approval.
- All remaining route/task/table/config ownership, compatibility removal, security/performance/
  reliability/resource evidence, SBOM, vulnerability and License gates remain pending.

Blocker:

- No explicit approval has been received for the five P11 contract bundles. Implementing any of
  them now would guess business, destructive-operation, concurrency or compatibility semantics.

Next:

- After explicit approval, implement `check-expired-users` first with application, PostgreSQL 18,
  Redis/Celery, import-sentinel, Manifest and focused-review evidence, then create its independent
  checkpoint. Keep P11 `DOING`, P12 `TODO` and Live Acceptance `HUMAN_PENDING` until the full local
  exit criteria pass.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Blocked-Audit

Stage / Batch: P11 / contract-approval blocked audit

HEAD / Commit:

- Pre-record HEAD: `394b306b` (`docs(p11): record contract decision blockers`).
- This docs-only checkpoint aligns the local status truth with the Goal blocked audit; it changes no
  runtime contract and does not authorize P12.

Evidence:

- The same concrete condition persisted across three consecutive Goal turns: no explicit approval
  was received for the five recorded P11 contract bundles, and read-only audits found no independent
  decision-free semantic closure.
- Current worktree preflight remained clean at
  `394b306b4bb4e6dcc1076c238714e5fbc9dba66c`; `git diff --check` passed.
- P11 remains incomplete at native-only Worker `17 executable / 33 unavailable`, routes `286 / 343`
  decorators and `306 / 367` method bindings native-owned, tables `27 new / 12 legacy`, and
  standalone settings `35 new / 13 legacy / 5 retired-approved`.

Blocker:

- Implementing the next slice without approval would guess user-expiry state and cancellation,
  destructive 115 organize planning, daily-theme failure/concurrency, intro provider/export, or
  quota-accounting semantics.

Resume condition:

- Explicit approval of the recommended P11 contract package. Resume as a fresh blocked audit,
  restore P11 to `DOING`, implement `check-expired-users` first, and keep P12 `TODO` plus Live
  Acceptance `HUMAN_PENDING` until the full P11 exit criteria pass.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-User-Expiry

Stage / Batch: P11 / processor-free durable user-expiry Worker slice

HEAD / Commit:

- Pre-slice HEAD: `d44cc98c` (`docs(p11): mark contract approval blocker`).
- Target checkpoint message: `refactor(p11): execute native user expiry`.

Implemented:

- Applied the approved P11 contract package and restored P11 from `BLOCKED` to `DOING`.
- Added deterministic per-parent-Job/per-user `EXPIRE_USER` operations, parent Job creation-time
  cutoff, authoritative Emby Policy readback, readback-only recovery after an uncertain write,
  local `expired` CAS, identity Outbox recovery and a fail-closed Alembic kind migration.
- Linearized renewal against the provider-write checkpoint with the existing user advisory scope:
  renewal committed before the checkpoint cancels the child without an Emby write; after the
  checkpoint the HTTP expiration update returns `409` until the child is reconciled.
- Made parent replay include previously created children after local status changes, preserved
  confirmed `expired` state during user sync, removed the `tasks.users` execution fallback and made
  both Worker profiles processor-free for this task.
- Updated the task ownership Manifest, ownership override, OpenAPI `409` response, HTTP fixture and
  the Alembic head assertions to `0022_p11_identity_user_expiry`.

Targeted gate:

- `uv lock --check`; scoped Ruff check/format; scoped Pyright; compileall; `git diff --check` ->
  clean. Application and architecture scope -> `153 passed`.
- Disposable PostgreSQL 18 identity expiry/migration/users/sync scope -> `43 passed`; cross-stage
  migration-head regressions -> `6 passed`.
- Disposable Redis/Celery native-only, compatibility-enabled, duplicate-delivery and replacement-
  Worker replay scope -> `3 passed`; the provider Stub received exactly one Policy write after a
  crash between child commit and parent Job result persistence.
- Manifest/OpenAPI currentness, single Alembic head and all three fixture/contract secret scans ->
  clean. Detailed evidence: `/tmp/etk-refactor-artifacts/p11-user-expiry-20260829/`.

Focused review:

- Initial review found a parent-replay P1 (confirmed children disappeared from the candidate query)
  and a renewal/provider-write P1 (the advisory lock did not fence a renewal after the checkpoint).
  Both were corrected and covered by PostgreSQL 18 plus Redis/Celery replacement-Worker evidence.
- The final review found no unresolved P0/P1/P2 in this slice. Data mutation, operation terminal
  state and Outbox completion share one UoW; no transaction is held across the Emby Stub call.

Pending:

- Continue the other approved P11 contracts: `intro-fingerprint-backfill`, `scan-organize-115`,
  `update-daily-theme`, then `subscription_quota_state`; ownership 100%, controlled legacy removal
  and all P11 NFR/security/SBOM/License gates remain pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Intro-Backfill

Stage / Batch: P11 / processor-free durable intro-fingerprint backfill Worker slice

HEAD / Commit:

- Pre-slice HEAD: `cb909460` (`refactor(p11): execute native user expiry`).
- Target checkpoint message: `refactor(p11): execute native intro backfill`.

Implemented:

- Replaced Worker and API wiring for intro policy, candidate, favorite and task reads with native
  PostgreSQL owners. The backfill task always uses `force=True`; only favorite mode is forceable,
  while off and playback remain native skips.
- Frozen `account_id` and the first available configured provider backend into each immutable intro
  operation. Direct URL extraction, refresh and continuation use only that backend; URLs remain
  operation-local memory and are absent from request, progress, result and Outbox payloads.
- Added exact favorite projection with processor-free authoritative Emby readback fallback, distinct
  library and active-watchlist candidate scopes, and native policy revalidation.
- Staged cache chapters, the Bridge effect and stable `sharing.intro.export` effect in one UoW. The
  P9 effect uses SHA1 aggregate identity and chapter-content dedup, validates marker ordering and
  malformed confirmations, and never controls P6 operation terminalization.
- Registered the export effect exactly once in both Worker profiles, promoted
  `intro-fingerprint-backfill` to native-only execution and recorded its native task owner.
- Old persisted operations without a frozen backend now fail closed immediately as
  `provider_backend_unpinned`; no backend is guessed and no policy/provider work or retry loop occurs.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  compileall and `git diff --check` -> clean.
- Intro/P4/P6/P9 application, architecture and task-contract scope -> `228 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 intro repository/runtime scope -> `10 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only and compatibility-enabled profiles ->
  `2 passed, 27 deselected`; disabled policy, duplicate delivery, no legacy task Processor,
  no intro operation/Outbox/export effect and no provider write were confirmed.
- Manifest/OpenAPI currentness and all three fixture/contract secret scans -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-intro-backfill-20260830/`.

Focused review:

- Initial review found one P1: pre-pin persisted operations would repeatedly retry because their
  request lacks `provider_backend`. The explicit fail-closed terminal path and forbidden
  policy/provider regression closed it. Delta rereview found no remaining P0/P1/P2.

Pending:

- Continue the approved `scan-organize-115`, `update-daily-theme` and
  `subscription_quota_state` contracts, then reach ownership 100%, perform controlled legacy
  removal and run all P11 NFR/security/SBOM/License gates. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Organize-Execution

Stage / Batch: P11 / approved immutable organize execution cutover

HEAD / Commit:

- Pre-slice HEAD: `da6fb81d` (`refactor(p11): execute native intro backfill`).
- Target checkpoint message: `refactor(p11): execute native organize plans`.

Implemented:

- Replaced API and both Worker profiles' legacy organize mutation client with an account-bound,
  backend-pinned native HTTP transport. OpenAPI and Cookie mkdir/move/rename use fixed endpoints and
  form contracts, no POST retry, credential refresh, redirect or backend fallback.
- Fenced account, access token or Cookie, and Cookie app type before and after every mutation. Only
  pre-request drift is retryable as proved no-effect; any post-request/transport/5xx/invalid response
  is resolved only by same-backend authoritative readback and otherwise becomes indeterminate.
- Added the real mutation-capable execution service to native-only Worker wiring and promoted
  `scan-organize-115` only after that graph was executable. Durable Jobs still carry only the frozen
  `execution_id`; the Plan remains the authority for target CID and actions.
- Removed the task from generic manual, chain and TG menus. Generic Job submission, stale configured
  chains, direct TG commands, legacy task-manager calls and old automatic triggers now fail closed;
  only `POST /api/p115/organize/plans/{plan_id}/execute` can reserve a new execution Job.
- Corrected the review-discovered Telegram/HDHive follow-up: an already successful P115 transfer no
  longer submits the retired organizer and therefore no longer converts success into a failed Job.
  Success and replay now return `organize_submitted=false`, require an approved Plan, and still
  destroy the operation secret.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  compileall and `git diff --check` -> clean.
- Organize/P4/P6/TG application, architecture and task-contract scope -> `225 passed` after review.
- Disposable PostgreSQL 18 organize execution/plan/records scope -> `17 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only and compatibility-enabled organize
  reconciliation profiles -> `2 passed, 27 deselected`.
- Frontend `typecheck` and production build -> passed; Manifest/OpenAPI currentness and all three
  fixture/contract secret scans -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-organize-execution-20260830/`.

Focused review:

- Initial entrypoint and transport audits identified the destructive legacy transport, empty-payload
  bypasses and non-executable native-only graph; all were closed before promotion.
- Delta review then found one P1 in the Telegram/HDHive post-transfer organizer follow-up. It was
  removed and covered for both share-import and offline-add success/replay. Final delta rereview found
  no remaining deterministic P0/P1/P2 in this slice.

Pending:

- Continue the approved `update-daily-theme` and `subscription_quota_state` cutovers, then reach
  ownership 100%, perform controlled legacy removal and run all P11 NFR/security/SBOM/License gates.
  P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Daily-Theme

Stage / Batch: P11 / processor-free atomic daily-theme publication and replenishment

HEAD / Commit:

- Pre-slice HEAD: `1733aceb` (`refactor(p11): execute native organize plans`).
- Target checkpoint message: `refactor(p11): publish daily theme atomically`.

Implemented:

- Replaced `update-daily-theme` Worker execution with a native service that retains the last valid
  pool on provider/projection failure, publishes valid empty results, supports same-theme
  replenishment and never imports the legacy task in either Worker profile.
- Added one PostgreSQL revision fence over pool, theme index, page and publication metadata. Daily
  publication, Portal removal and durable per-publication receipts share the same advisory/table
  lock and UoW; stale plans cannot overwrite a later publication or removal.
- Made low-inventory Portal removal reserve `daily-theme.replenishment` plus its Outbox event in the
  Portal UoW. The effect carries only the trigger revision and uses its own UUID as the idempotent
  publication receipt identity.
- Added replay-safe stale-claim recovery only for the local replenishment effect. A crash before
  publication rearms the effect; a crash after commit replays the durable receipt and confirms the
  original effect. External media-effect types retain their no-blind-replay `indeterminate` rule.
- Tightened the trigger to an explicit `202` schema with required `job_id`, synchronized the current
  OpenAPI TypeScript schema, and made the frontend accept an authoritatively published empty pool.
- Recorded the native task and settings owners while keeping legacy direct accesses explicit as
  compatibility residuals until controlled removal.

Targeted gate:

- Scoped Ruff and format -> clean (`29 files already formatted`); scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`; `uv lock --check`, compileall and `git diff --check` ->
  clean.
- Daily-theme/media-effect/application/architecture/contracts scope -> `165 passed, 1` existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 media-effect, daily-theme and Portal scope -> `41 passed`; generic
  external stale claims remain indeterminate while the opted-in local effect is rearmed.
- Disposable PostgreSQL 18 + Redis/Celery native-only and compatibility-enabled profiles ->
  `2 passed, 29 deselected`; Job receipt replay and both replenishment crash windows were covered.
- Frontend OpenAPI generation was byte-stable before/after regeneration; `typecheck`, production
  build and API-boundary check passed. Manifest/OpenAPI currentness, all three secret scans and
  source compilation passed.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-daily-theme-20260830/`.

Focused review:

- Initial review closed provider partial-publication, empty-result polling, latest-state-only replay,
  nullable trigger identity and post-commit replenishment-intent gaps.
- Storage review then found one P1: the local replenishment effect inherited external-effect stale
  claim terminalization. The opt-in replay-safe recovery and PostgreSQL/Redis-Celery crash-window
  regressions closed it without weakening any external effect fence.
- Generated client churn is the deterministic output of the current full OpenAPI, not a manual or
  unrelated backend change; regeneration preserved the exact SHA-256.

Pending:

- Complete the approved `subscription_quota_state` ledger-only cutover, then reach 100% executable
  ownership, produce pre-removal evidence, perform controlled legacy removal and run all P11
  NFR/security/SBOM/License gates. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Quota-Ledger

Stage / Batch: P11 / subscription quota ledger-only cutover

HEAD / Commit:

- Pre-slice HEAD: `38a5c2cd` (`refactor(p11): publish daily theme atomically`).
- Target checkpoint message: `refactor(p11): cut over subscription quota ledger`.

Implemented:

- Made `subscription_quota_reservations` the sole accounting fact. Compatibility quota reads and
  all native terminal transitions aggregate only `reserved` and `committed`; the
  `subscription_quota_state` row is a derived compatibility projection and cannot authorize,
  reserve, commit or release capacity.
- Made `mp_config.resubscribe_daily_cap` the canonical cap source, with an existing reservation's
  `cap_snapshot` used only when the canonical setting is absent or invalid. A tampered projection
  no longer changes accounting and is repaired from the ledger on compatibility read.
- Disabled direct projection decrement. Quota-consuming MoviePilot fallback and non-persistent
  HDHive resubscribe now fail before provider access unless a durable operation gateway owns the
  write; the unreachable legacy HDHive inline block was removed.
- Preserved Subscribe Assistant repair and full-washing recovery as non-quota-consuming
  reconciliation paths. Added source sentinels and behavior regressions for the compatibility
  getter, direct decrement, provider fail-closed boundary and recovery defaults.
- Promoted the compatibility projection to its native repository owner in the configuration
  Manifest while explicitly documenting that it is not an accounting source.

Targeted gate:

- Native/test scoped Ruff and format, legacy import-only Ruff, scoped Pyright, source compilation,
  `uv lock --check` and `git diff --check` -> clean.
- Subscription/resubscribe/assistant application and architecture scope -> `42 passed`.
- Disposable PostgreSQL 18 quota/runtime/operation scope -> `20 passed`, including concurrency at
  cap, reservation replay, rollback, dynamic canonical cap, projection tamper through the real
  compatibility getter, indeterminate quota retention and pre-write release.
- Manifest/OpenAPI currentness and all three fixture/contract secret scans -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-quota-ledger-20260830/`.

Focused review:

- Initial findings-first review identified four P2 evidence/residual gaps: an unreachable direct
  decrement block, helper-only tamper coverage, no behavior test for direct decrement and no
  explicit recovery-default sentinel. All four were closed with the smallest source and test
  changes. Final local review found no remaining deterministic P0/P1/P2 in this slice.

Pending:

- Quota accounting ownership is complete, but `auto-subscribe`, `manual_subscribe_batch`,
  `resubscribe-library` and `subscribe-assistant-maintenance` still require processor-free native
  execution before task ownership can reach 100%. Route/Table/Config/Frontend closure, pre-removal
  inventory, controlled legacy removal and all P11 NFR/security/SBOM/License gates remain pending.
  P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Manual-Subscribe

Stage / Batch: P11 / processor-free durable manual subscription execution

HEAD / Commit:

- Pre-slice HEAD: `5dba5dec` (`refactor(p11): cut over subscription quota ledger`).
- Target checkpoint message: `refactor(p11): execute native manual subscriptions`.

Implemented:

- Added a processor-free `ManualSubscriptionBatchService` for Movie, Season and Series requests,
  with strict task envelopes, cancellation checkpoints, legacy-compatible counters and explicit
  handling of invalid Season identity. Series expansion preserves future-season
  `PENDING_RELEASE`, TG-owned local success and create-then-pending provider sequencing.
- Bound durable MoviePilot effects to explicit Job/Attempt/Worker identity, preserving operation,
  quota and Outbox replay without relying on compatibility `task_manager` execution context.
- Promoted `manual_subscribe_batch` to native-only Worker execution in both runtime profiles and
  migrated the Portal from callable submission to catalog task key plus JSON parameters. Season
  Portal requests now carry an authoritative parent Series identity.
- Updated native execution ownership and generated manifests; retained the legacy callable only as
  source compatibility pending the controlled removal wave.

Targeted gate:

- Native/test scoped Ruff and format -> clean; legacy delta safety Ruff -> clean; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`; source compilation, `uv lock --check` and
  `git diff --check` -> clean.
- Manual subscription/P4/P7 application and architecture scope -> `198 passed`, with one existing
  Starlette WSGI deprecation warning.
- Disposable PostgreSQL 18 manual subscription, operation and assistant runtime scope ->
  `34 passed`.
- Disposable PostgreSQL 18 + Redis/Celery native-only and compatibility-enabled Worker profiles,
  including duplicate delivery and replacement Worker replay -> `2 passed, 31 deselected`.
- Manifest/OpenAPI currentness and all three fixture/contract secret scans -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-manual-subscribe-20260830/`.

Focused review:

- Review found missing explicit Season parent validation and an unclassified empty Series detail;
  both were closed with business-skip regressions. Follow-up found the Portal Season payload lacked
  the newly required parent identity; the payload and source sentinel were fixed. Final rereview
  found no remaining deterministic P0/P1/P2 in this slice.
- Five other observations were retained as frozen legacy compatibility behavior rather than changed
  without a contract decision: Movie detail failure skip, Series-without-seasons provider-only
  success, missing `episode_count` default, quota precheck/succeeded semantics and Job-scoped replay.

Pending:

- `auto-subscribe`, `resubscribe-library` and `subscribe-assistant-maintenance` still require
  processor-free native owners. Other unavailable P5/P7/P8 task owners, remaining Route/Table/
  Config closure, pre-removal inventory, controlled legacy removal and all P11 NFR/security/SBOM/
  License gates remain pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Auto-Subscribe-Foundation

Stage / Batch: P11 / processor-free auto-subscribe policy and worklist repository foundation

HEAD / Commit:

- Pre-slice HEAD: `69ff456b` (`refactor(p11): execute native manual subscriptions`).
- Target checkpoint message: `refactor(p11): isolate auto subscription policy`.

Implemented:

- Isolated the auto-subscribe policy behind typed settings, worklist, quota, MoviePilot, ordered
  source and notification ports without importing legacy task/database/handler modules.
- Preserved Movie/Season worklist ordering and missing-episode aggregation, PENDING promotion,
  movie revive/pause repair, WANTED-only release/quota protection, non-WANTED MP fencing, configured
  source order, legacy result counters and best-effort notification aggregation.
- Added an explicit indeterminate source disposition that stops fallback, while confirmed later
  sources can supersede an earlier rejected MP attempt. Cancellation checkpoints now surround
  item/source/provider, delay and notification boundaries.
- Added a SQLAlchemy repository for lifecycle projections and the exact Movie/Season worklist SQL.
  This foundation is intentionally not wired to Worker and does not change task ownership yet;
  stable item claims/occurrences and production shared/HDHive/TG adapters remain the next batch.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  `git diff --check` -> clean.
- Auto-subscribe policy application matrix -> `11 passed`.
- Disposable PostgreSQL 18 worklist/lifecycle repository matrix -> `2 passed`.

Focused review:

- Initial review found MP fallback termination, untyped source exceptions, disabled-maintenance
  parsing, delay-on-skip parity and pre-promotion cancellation defects; all were fixed with
  regressions. It also identified the required production cutover boundary: no Worker promotion
  before a stable per-item claim/occurrence and replay-safe source delivery exists.

Pending:

- Add the durable item claim/receipt boundary, production shared/HDHive/TG and notification adapters,
  explicit Job identity binding, native-only Worker wiring, PG18 provider/projection recovery and
  Redis/Celery duplicate/replacement evidence. Until then `auto-subscribe` remains a legacy
  execution owner and cannot be added to the native-only allowlist.
- All other P11 ownership, controlled removal and NFR gates remain pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Auto-Subscribe-Occurrences

Stage / Batch: P11 / durable auto-subscribe item occurrence and MoviePilot identity foundation

HEAD / Commit:

- Pre-slice HEAD: `4a8f012c` (`refactor(p11): isolate auto subscription policy`).
- Target checkpoint message: `refactor(p11): persist auto subscription occurrences`.

Implemented:

- Added deterministic worklist-generation occurrences, cross-Job item claims, leases and claim-token
  fencing over `subscription_runtime_states`. Active claims return busy, confirmed generations replay
  locally, replay-safe expired requests can recover, and expired non-replay-safe requests become
  indeterminate instead of being replayed blindly.
- Bound MoviePilot create/status effects to the stable occurrence. Their scope and durable request
  origin no longer depend on the parent Job when an explicit occurrence is present; existing manual
  and deletion call sites retain their prior identity behavior.
- Added a dedicated auto MoviePilot adapter for Movie/Season, including pending create-then-status,
  completed washing flags, TG-owned non-completed seasons and quota exhaustion. Provider confirmation
  and the local media projection/receipt now complete in one repository UoW.
- Added the native Telegram notification directory and best-effort delivery adapter. Production
  Shared Center, HDHive and TG source execution is intentionally not wired: the current P9 sharing
  operation code has a repository but no provider executor/readback consumer, so promotion would be
  an allowlist-only ownership claim.

Targeted gate:

- Auto policy/adapters/MoviePilot effect application scope -> `31 passed`.
- Disposable PostgreSQL 18 auto worklist/claim/projection scope -> `5 passed`, including two-Job
  claim fencing, stale-token rejection, replay-safe recovery, non-replay-safe indeterminate handling
  and injected projection rollback followed by recovery.
- Scoped Ruff check/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`);
  `git diff --check` -> clean.

Focused review:

- Confirmed the stable occurrence, MP scope/origin split, claim-token fence and projection transaction.
  A zero-row projection guard and an injected rollback/recovery regression were added during review.
- No P0/P1/P2 remains in this foundation. It is not evidence for production source execution or
  native-only Worker ownership.

Stage full gate:

- Not run. P11 ownership, controlled removal and NFR/security/SBOM/License gates remain pending.

Pending:

- Implement a real provider executor/readback boundary for sharing operations, then native
  Shared Center/HDHive/TG auto sources, Router/Worker DI, Redis-Celery duplicate/replacement evidence,
  import sentinels, native-only promotion and task Manifest ownership.
- All other P11 Route/Task/Table/Config closure, controlled removal and NFR gates remain pending;
  P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Claim-Replay-Fix

Stage / Batch: P11 / sharing operation stale-request safety fix

HEAD / Commit:

- Pre-fix HEAD: `a31491b2` (`refactor(p11): persist auto subscription occurrences`).
- Target checkpoint message: `fix(p11): prevent stale sharing write replay`.

Implemented:

- Split stale sharing claims by the provider-write checkpoint. An expired pre-request `claimed`
  operation remains retryable and rearms its Outbox; an expired `requesting` operation now becomes
  terminal `indeterminate`, keeps the scope fenced and cannot be claimed or blindly replayed.
- Kept the transition and Outbox terminal/rearm update in one PostgreSQL UoW. The existing
  `sharing_claim_expired` error type remains stable for pre-request recovery; requesting expiry adds
  explicit `provider_may_have_been_called=true` evidence.

Targeted gate:

- P9 application scope -> `36 passed`.
- Disposable PostgreSQL 18 sharing ledger/operation scope -> `4 passed`, including pre-request
  requeue and post-checkpoint terminal fencing.
- Scoped Ruff check/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`);
  `git diff --check` -> clean.

Focused review:

- The current repository has no provider executor or authoritative readback consumer. Treating both
  stale states as retryable was therefore a deterministic duplicate-write risk. The fail-closed
  split removes that risk without claiming an executor exists. No P0/P1/P2 remains in this fix.

Pending:

- A native sharing provider executor/readback boundary is still required before auto Shared Center,
  HDHive or TG source execution can be promoted. P11 remains `DOING`; P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Operation-Recovery

Stage / Batch: P11 / durable sharing operation execution and recovery foundation

HEAD / Commit:

- Pre-slice HEAD: `5a983bd7` (`fix(p11): prevent stale sharing write replay`).
- Target checkpoint message: `refactor(p11): define sharing operation recovery`.

Implemented:

- Added a typed `SharingOperationExecutor` with an explicit provider-write checkpoint, authoritative
  readback-only recovery and fail-closed `indeterminate` handling when readback is unavailable.
- Split stale recovery into mutually exclusive paths: only proven pre-write claims can rearm the
  provider request; any claim carrying `step_started_at` can only re-enter authoritative readback.
- Added claim-token plus lease fencing to every state transition, a shared attempt ceiling,
  dead-dispatch fencing, deep durable-JSON outcome validation and atomic operation/Outbox updates.
- Kept the slice intentionally unwired from production providers, Worker routing and ownership
  manifests. It is recovery infrastructure, not evidence that `auto-subscribe` is native-only.

Targeted gate:

- Scoped Ruff and format -> clean (`5 files already formatted`); scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`; `uv lock --check`, scoped compilation,
  manifest currentness and `git diff --check` -> clean.
- P9 application scope -> `40 passed`.
- Disposable PostgreSQL 18 sharing ledger/executor scope -> `17 passed`, including duplicate
  Workers, checkpoint commit acknowledgement loss, expired-token fencing, pre-write attempt
  exhaustion, stale readback recovery, no-resend and Outbox terminal/rearm state.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-recovery-20260830/`.

Focused review:

- Initial review identified stale readback replay, competing recovery paths, missing lease fencing,
  checkpoint commit ambiguity, dead-dispatch fencing, attempt ceiling and malformed outcome gaps.
  The final rereview found no remaining deterministic P0/P1/P2 in this foundation.

Pending:

- Implement production Fake/Stub-backed Shared Center, HDHive and Telegram delivery adapters,
  recovery coordinator, Outbox/Worker wiring and `auto-subscribe` native-only execution before
  changing task ownership. Remaining task/route/table/config ownership, controlled removal and all
  P11 NFR gates remain pending; P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Operation-Dispatch

Stage / Batch: P11 / durable sharing operation provider dispatch

HEAD / Commit:

- Pre-slice HEAD: `8af51568` (`refactor(p11): define sharing operation recovery`).
- Target checkpoint message: `refactor(p11): dispatch sharing operations`.

Implemented:

- Added strict Shared Center, HDHive and Telegram Bot provider delivery, validated Celery publication,
  Coordinator recovery/Outbox dispatch and compatibility/native-only Worker execution.
- Kept request validation before the durable provider-write checkpoint. Post-checkpoint failure can
  only use an explicitly declared authoritative readback; nested request echoes cannot satisfy the
  identity match and provider payloads are reduced to safe identifiers plus a SHA-256 digest.
- Loaded the real standalone `shared_resource_config` and `hdhive_config` records in both Worker
  profiles and AppContainer adapters instead of assuming those keys lived in `dynamic_app_config`.
- Moved transfer operation reservation, contribution debit and Outbox creation into one PostgreSQL
  UoW; insufficient credit rolls all three back.

Targeted gate:

- Scoped Ruff check/format, Pyright, compileall, manifest currentness and `git diff --check` -> clean;
  Pyright reported `0 errors, 0 warnings, 0 informations`.
- P9 application plus P8 Worker-regression scope -> `71 passed`.
- Disposable PostgreSQL 18 sharing ledger/operation scope -> `19 passed`.
- Disposable PostgreSQL 18 + Redis/Celery + local HTTP Stub -> `2 passed` across both Worker profiles;
  Shared Center, HDHive and Telegram each wrote once, duplicate deliveries and a replacement Worker
  did not repeat them, and the real Worker construction read HDHive standalone credentials.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-dispatch-20260830/summary.md`.

Focused review:

- Fixed three deterministic P1 defects found during review: validation after checkpoint, missing
  standalone provider configuration in Worker DI, and arbitrary nested response identity matching.
- No remaining deterministic P0/P1/P2 exists inside this dispatch slice. The broader P11 stage still
  has known P1 gaps: synchronous Shared Center mutations, incomplete native Shared Resource routes,
  missing backend-owned Server ID identity, and heartbeat-only native Coordinator.

Pending:

- Migrate the native Shared Resource configuration/UI contracts and device identity, convert the
  remaining synchronous mutations to durable operations, add the protected HDHive receipt/source
  pipeline, make native-only Coordinator dispatch Outbox, then promote `auto-subscribe` only after
  its PG18/Redis/import-sentinel evidence passes.
- Remaining task/route/table/config ownership, controlled removal and all P11 NFR/security/SBOM/
  License gates remain pending. P11 stays `DOING`; P12 stays `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-Coordinator-Dispatch

Stage / Batch: P11 / native-only Coordinator Outbox dispatch

HEAD / Commit:

- Pre-slice HEAD: `315e1e5d` (`refactor(p11): dispatch sharing operations`).
- Target checkpoint message: `refactor(p11): dispatch native coordinator outboxes`.

Implemented:

- Replaced the native-only heartbeat with advisory-lock leadership and Job, Media Effect,
  ingestion, P115 upload/playback/intro, Subscription, Collection, Identity, Database Maintenance
  and Sharing Outbox dispatch.
- Kept native-only execution free of compatibility scheduler, RuntimeSupervisor, Watchdog ingress,
  Telegram UserBot and legacy runtime-context refresh. P115 upload startup only ensures its native
  control-plane configuration and does not scan directories or call a provider.
- Paired native Workers with the native Coordinator in the real Redis/Celery profile. Redis outage
  recovery and dual-Coordinator failover now exercise the native runtime rather than compatibility
  dispatch.
- Closed the focused-review P1 by placing partial native initialization inside the same cleanup
  boundary as the dispatch loop; initialization failure now closes the AppContainer and writes
  stopped health.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  application process-role/Coordinator scope -> `55 passed, 1` existing Starlette WSGI deprecation
  warning; `git diff --check` -> clean.
- Disposable PostgreSQL 18 + Redis/Celery sharing profiles, Redis outage recovery and native
  dual-Coordinator leader failover -> `4 passed, 31 deselected`.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-native-coordinator-20260830/summary.md`.

Focused review:

- Leadership acquisition/loss, shutdown, native/compat isolation, control-plane initialization,
  publisher topic separation and degraded publish handling were reviewed. The only P1 was the
  partial-startup resource leak described above; it is closed with a regression. No deterministic
  P0/P1/P2 remains in this slice.

Pending:

- Native Shared Resource HTTP/UI contracts, backend-owned device identity, remaining synchronous
  provider mutations, protected HDHive/share receipts and `auto-subscribe` source execution remain.
- Remaining Task/Route/Table/Config/Frontend ownership, pre-removal evidence, controlled legacy
  removal and all P11 NFR/security/SBOM/License gates remain pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Center-Browse

Stage / Batch: P11 / native Shared Center connection and display browse

HEAD / Commit:

- Pre-slice HEAD: `54afd8e0` (`refactor(p11): dispatch native coordinator outboxes`).
- Target checkpoint message: `refactor(p11): migrate shared center browsing`.

Implemented:

- Added canonical native `shared_resource_config` normalization over `app_settings`, including
  legacy default home sections, bounded section policy, Rapid mode and partial-update preservation.
- Added cache-first backend-owned Emby Server ID resolution. Only the SHA-256 digest is sent to the
  Shared Center; the frontend cannot supply or observe the raw identity.
- Wired the same identity/configuration source into AppContainer, compatibility Worker and
  native-only Worker.
- Added native config GET/POST, backend-owned device registration/status and display tags/home/
  children/detail routes; changed source listing to the display-list contract.
- Added typed adapter methods with exact `sections_json` and ordered `source_ids` serialization,
  display snapshot decoration and lazy season-pack slimming.
- Promoted exactly six implemented route bindings, five decorators and the standalone
  `shared_resource_config` setting. Ownership is now `291 / 343` route decorators, `312 / 367`
  method bindings and `38 new / 11 legacy / 5 retired-approved` standalone settings.

Targeted gate:

- Scoped Ruff and format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`;
  P9 application scope -> `75 passed`; `git diff --check` -> clean.
- Disposable PostgreSQL 18 sharing ledger/config/identity/operation scope -> `20 passed`.
- Disposable PostgreSQL 18 + Redis/Celery sharing dispatch through both Worker profiles ->
  `2 passed`.
- Manifest -> clean; OpenAPI -> clean; generated TypeScript client, frontend typecheck, build and
  API-boundary check -> passed. Build retains only the existing large-chunk warning.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-connection-20260830/summary.md`.

Focused review:

- Closed the review-discovered home-section P1 and the pack detection, JSON-string metadata,
  ongoing-completion, short-drama override, malformed count, transient identity status and
  reachable-202 P2 findings with targeted regressions.
- Provider-confirmed device registration followed by a local enable-state save failure now remains
  HTTP 200 with `configuration_saved=false`; it does not invite a duplicate provider registration.
- No deterministic P0 remains in this slice. The broader stage P1 is explicit: device/source/
  transfer/share-request mutations still include synchronous provider paths without durable
  operation/readback ownership.

Pending:

- Complete durable Shared Center mutations and the local shared-source/credit/share-request/import
  projections and remaining UI routes.
- Native-only Worker remains `22 executable / 28 unavailable`; tables remain `27 new / 12 legacy`;
  bootstrap/dynamic settings and all other legacy ownership remain incomplete.
- Pre-removal inventory, controlled legacy removal and all P11 NFR/security/SBOM/License gates
  remain pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, shared center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Center-Mutations

Stage / Batch: P11 / durable Shared Center core mutations

HEAD / Commit:

- Pre-slice HEAD: `a26b37ff` (`refactor(p11): migrate shared center browsing`).
- Target checkpoint message: `refactor(p11): persist shared center mutations`.

Implemented:

- Converted device/source registration, transfer lease/report and share-request creation from
  synchronous request-thread provider writes to durable `sharing_operations` plus Outbox delivery;
  the quote endpoint remains a retryable read-only query.
- Added authenticated principals, stable scope identity, deterministic or explicit idempotency,
  conflict responses, device authoritative readback and safe provider receipts. The frontend now
  retains one UUID across uncertain retries and polls the durable operation to terminal state.
- Added atomic device-enable continuation and transfer escrow compensation for provider rejection,
  dead dispatch and exhausted pre-write claims. Compensation requires the exact original debit,
  commits exactly once and never releases an indeterminate escrow.
- Added the explicit `register_device` kind in the single Alembic head
  `0023_p11_sharing_device_kind`; regenerated Manifest, OpenAPI and the TypeScript schema.

Targeted gate:

- P9 application -> `81 passed`; scoped Ruff/format -> clean; scoped Pyright ->
  `0 errors, 0 warnings, 0 informations`; compileall, `uv lock --check` and `git diff --check` ->
  clean.
- Disposable PostgreSQL 18 sharing ledger/operation scope -> `28 passed`. The initial three
  compensation failures were traced to the ledger's SHA-256 idempotency-key normalization; the
  shared normalization repair retained strict debit identity checks and the focused rerun passed
  `4` cases.
- Disposable PostgreSQL 18 + Redis/Celery through native-only and compatibility-enabled Worker
  profiles -> `2 passed, 33 deselected`.
- Frontend typecheck and build passed; Vitest -> `7 passed`; API-boundary -> clean. Manifest,
  OpenAPI, single Alembic head and all three fixture/contract secret scans are clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-mutations-20260830/summary.md`.

Focused review:

- Request threads reserve operations only; operation/Outbox/debit share one UoW; terminal local
  continuation and compensation roll back with the terminal transition on failure.
- Missing or mismatched debit fails closed, compensation is exactly once, indeterminate retains
  escrow, and a checkpointed provider request can only reconcile without resend.
- Session principal, stable scope conflict, safe receipt filtering, frontend idempotency retention
  and migration single-head boundaries are covered. No deterministic P0/P1/P2 remains in this
  slice.

Pending:

- Sharing operation cancellation, remaining Shared Resource projections/UI routes and background
  tasks remain, followed by the other unavailable native tasks and Route/Table/Config ownership.
- Pre-removal evidence, controlled legacy removal and all P11 NFR/security/SBOM/License gates remain
  pending. P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Custom-Collection-Refresh

Stage / Batch: P11 / processor-free custom collection refresh

HEAD / Commit:

- Pre-slice HEAD: `8eaf0804` (`refactor(p11): cut over moviepilot webhook`).
- Target checkpoint message: `refactor(p11): execute native custom collection tasks`.

Implemented:

- Replaced the three custom collection task callables (`custom-collections`,
  `process_all_custom_collections`, and `process-single-custom-collection`) with one shared
  `CustomCollectionTaskService` used by both native-only and compatibility-enabled Worker profiles.
- Added typed source, TMDb, media-index, library-query and recommendation adapters while preserving
  list, RSS, TMDb list/discover, Douban, Maoyan, global recommendation, filter and personal
  recommendation behavior, including multi-URL ordering, dynamic dates, season identity and
  visible placeholder fallback.
- Added durable `collection.custom.refresh` operations with frozen definition/source plans, Emby
  create/add/remove/delete phases, claim/recovery, provider readback, no-resend after checkpoint,
  hard-ignore fencing, and atomic custom-collection/subscription/projection/Outbox completion.
- Regenerated task ownership artifacts; the dirty manifest now has 17 remaining
  `legacy-compatibility-callable` task owners (the committed pre-slice manifest had 20).

Targeted gate:

- Custom application suite -> `50 passed`.
- Disposable PostgreSQL 18 custom refresh integration -> `15 passed`.
- Disposable Redis/Celery native-only and compatibility-enabled Worker profiles -> `2 passed, 39 deselected`.
- Scoped Ruff, format check, `uv run --frozen pyright`, compileall, `uv lock --check`, manifest check,
  task export, Compose config and `git diff --check` passed.
- Docker Server `29.4.0` was available for the disposable gates; no production container or external
  service was used.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-custom-collection-refresh-20260902/summary.md`.

Pending:

- Complete the remaining 17 processor-free task owners in the three shared capability batches, then
  finish Table/Config/Frontend ownership, pre-removal, controlled removal and P11
  NFR/security/SBOM/vulnerability/License gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Cancellation

Stage / Batch: P11 / durable sharing operation cancellation

HEAD / Commit:

- Pre-slice HEAD: `c70f0ad4` (`refactor(p11): persist shared center mutations`).
- Target checkpoint message: `refactor(p11): add sharing operation cancellation`.

Implemented:

- Added terminal pre-provider cancellation and durable post-checkpoint cancellation intent to
  `sharing_operations`, with one PostgreSQL UoW for operation state, Outbox completion and exact
  transfer escrow compensation.
- Preserved the provider-write fence after `step_started_at`: cancellation no longer releases
  escrow or permits resend, and authoritative readback may still confirm the operation or exhaust
  to `indeterminate`.
- Added the authenticated cancellation API, terminal replay/conflict semantics, Alembic revision
  `0024_p11_sharing_cancellation`, generated OpenAPI/TypeScript contracts and dual-Worker replay
  coverage.

Targeted gate:

- P9 application -> `80 passed`; disposable PostgreSQL 18 P9 sharing scope -> `31 passed`.
- Migration-head architecture regressions -> `22 passed`; PostgreSQL 18 upgrade/downgrade/head
  regressions across P3/P7/P9 -> `5 passed`.
- Disposable PostgreSQL 18 + Redis/Celery through native-only and compatibility-enabled Worker
  profiles -> `2 passed, 33 deselected`; a cancelled transfer produced no provider request, its
  Outbox was terminal and its escrow was restored exactly once under duplicate/replacement Worker
  delivery.
- Scoped Ruff/format, Pyright (`0 errors, 0 warnings, 0 informations`), compileall, lock, Alembic
  single-head and `git diff --check` -> clean.
- Frontend typecheck/build/API boundary passed; Vitest -> `7 passed`. Manifest, OpenAPI and all
  three secret scans are clean. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-sharing-cancellation-20260830/summary.md`.

Focused review:

- Cancellation/checkpoint linearization, claimed Outbox delivery, post-checkpoint no-resend,
  confirmed/readback exhaustion, escrow release/retention, terminal replay and migration downgrade
  were reviewed. No deterministic P0/P1/P2 remains in this slice.

Pending:

- The non-targeted full application run currently has three stage-wide failures: the P1 import
  subprocess exits early, and P5/P7 tests retain stale Emby-search and `shared_resource_config`
  ownership expectations. They are not cancellation failures but must be reconciled before the P11
  full gate.
- Continue with native Shared Resource local projections/API/UI, then background task execution and
  all remaining Task/Route/Table/Config ownership. Pre-removal inventory, controlled removal and all
  P11 NFR/security/SBOM/License gates remain pending; P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Application-Gate-Repair

Stage / Batch: P11 / stage-wide application import and contract regression repair

HEAD / Commit:

- Pre-slice HEAD: `f9b8a4f2` (`refactor(p11): add sharing operation cancellation`).
- Target checkpoint message: `fix(p11): keep plaintext imports side-effect free`.

Implemented:

- Deferred `p115client` construction until an actual direct-URL request. Importing `main` and the
  plaintext app factory no longer triggers the SDK's `$HOME` cache-directory side effect.
- Preserved injectable SDK constructors and reconciled the P5 Emby-search, P7
  `shared_resource_config` and P6 dual-profile direct-URL assertions with already-migrated native
  ownership.

Targeted gate:

- P1 import/CLI -> `4 passed`; direct-URL adapter -> `13 passed`; focused P5/P7 contracts ->
  `2 passed`; P6 media-info ownership -> `8 passed`.
- Full application gate -> `1864 passed, 1` existing Starlette WSGI deprecation warning.
- Scoped Ruff/format, Pyright (`0 errors, 0 warnings, 0 informations`), compileall and
  `git diff --check` -> clean. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-application-gate-20260830/summary.md`.

Focused review:

- The failure was an import-time filesystem side effect in a third-party SDK, not a missing test
  directory. The default SDK factories are now lazy while explicit test/runtime factories retain
  their existing behavior. No deterministic P0/P1/P2 remains in this repair.

Pending:

- Continue with native Shared Resource local projections/API/UI, then background task execution and
  all remaining Task/Route/Table/Config ownership. Pre-removal inventory, controlled removal and all
  P11 NFR/security/SBOM/License gates remain pending; P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Resource-Projections

Stage / Batch: P11 / native Shared Resource local and read projections

HEAD / Commit:

- Pre-slice HEAD: `bcadbe43` (`fix(p11): keep plaintext imports side-effect free`).
- Target checkpoint message: `refactor(p11): migrate shared resource projections`.

Implemented:

- Added the native SQLAlchemy sharing projection for local source summary/list/check, account-bound
  media search, local manual RAW validation, virtual-import listing, static share-request options and
  typed TMDb search. Request-thread projection paths do not call Shared Center or perform provider
  writes.
- Preserved Episode-to-Season aggregation, completed logical-season channel association, UI status
  filters, per-page RAW loading, account-scoped 115 cache/ancestor reads and multi-file PickCode-to-
  SHA1 identity. RAW validation accepts ffprobe, nested `MediaSourceInfo.MediaStreams` and top-level
  `MediaStreams` shapes while rejecting audio-only content.
- Registered nine native HTTP operations, regenerated OpenAPI and the TypeScript schema, and moved
  only those verified bindings/decorators to the native projection owner. Stabilized the P9 proxy
  lease test by supplying its existing fixed clock to `refresh()`.

Targeted gate:

- P9 application -> `82 passed`; disposable PostgreSQL 18 P9 scope -> `39 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`; scoped Ruff/format and Pyright -> clean
  (`0 errors, 0 warnings, 0 informations`); compileall, `uv lock --check` and `git diff --check` ->
  clean.
- Frontend typecheck/build/API boundary passed; Vitest -> `7 passed`; build retained only the
  existing large-chunk warning. Manifest, OpenAPI and all three fixture/contract secret scans are
  clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-projection-20260830/`.

Focused review:

- Reviewed RAW shape and display requirements, multi-file identity, account isolation, logical-
  season channel association/status filters, pagination, auth/status-code contracts and generated
  ownership artifacts. A final review found sparse SHA1-slot misalignment (P1) and synthetic
  cross-account ancestor fallback (P2); slot-preserving identity plus fail-closed ancestry and
  PostgreSQL 18 regressions closed both. No deterministic P0/P1/P2 remains in this slice.

Pending:

- Shared Resource credit refresh, share-request co-request/cancel, source lifecycle, manual-create
  completion, center import/virtual effects and sharing background task execution remain.
- Continue all unavailable native tasks and Route/Task/Table/Config ownership, then generate the
  pre-removal inventory, perform controlled removal and run all P11 NFR/security/SBOM/License gates.
  P12 remains `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Participation

Stage / Batch: P11 / durable share-request participation

HEAD / Commit:

- Pre-slice HEAD: `1471c43e` (`refactor(p11): migrate shared resource projections`).
- Target checkpoint message: `refactor(p11): persist share request participation`.

Implemented:

- Converted Shared Center co-request participation and remote share-request cancellation into typed,
  single-shot provider deliveries reserved as durable `SHARE_REQUEST` operations plus Outbox; request
  threads no longer call those provider mutations directly.
- Scoped operations by group and authenticated principal, preserved stable idempotency replay and
  cross-action conflict behavior, and retained no-resend `INDETERMINATE` semantics because no frozen
  authoritative participant readback contract exists.
- Added the two native FastAPI routes, regenerated OpenAPI/TypeScript contracts and promoted exactly
  those verified ownership entries. Route ownership is now `302 new / 41 legacy` decorators and
  `323 new / 44 legacy` method bindings.
- Updated the Shared Resource UI to retain the occurrence key across uncertain retries, wait for
  `CONFIRMED`, show terminal success text, and preserve explicit indeterminate/error reasons.

Targeted gate:

- Scoped Ruff/format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- P9 application -> `85 passed`; disposable PostgreSQL 18 P9 scope -> `40 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`; compileall, `uv lock --check` and
  `git diff --check` -> clean.
- Frontend typecheck/build/API boundary passed; Vitest -> `7 passed`; build retained only the
  existing large-chunk warning. Manifest, OpenAPI and all three fixture/contract secret scans are
  clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-sharing-participation-20260830/summary.md`.

Focused review:

- Closed two UI P2 findings: queued text was no longer presented as terminal success, and polling
  failures no longer discard the explicit indeterminate reason.
- Checkpoint ordering, unknown/no-resend behavior, scope/replay conflicts, terminal Outbox UoW and
  remote-group versus local-operation cancellation semantics were reviewed. No deterministic
  P0/P1/P2 remains in this slice.

Pending:

- Shared Resource source lifecycle, center import, virtual effects, credit refresh and maintenance
  routes remain, along with all unavailable native tasks and Route/Table/Config ownership.
- Pre-removal inventory, controlled legacy-runtime removal and all P11 NFR/security/SBOM/
  vulnerability/License gates remain pending. P11 stays `DOING`; P12 stays `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Source-Lifecycle

Stage / Batch: P11 / durable local shared-source disable and delete

HEAD / Commit:

- Pre-slice HEAD: `e4c9d9f5` (`refactor(p11): persist share request participation`).
- Target checkpoint message: `refactor(p11): persist shared source lifecycle`.

Implemented:

- Migrated single and batch local-source disable/delete routes, including both POST and DELETE
  compatibility methods. Center-linked sources now reserve a typed, single-shot `disable_source`
  operation; local source/file state changes only after provider confirmation.
- Kept operation terminal state, local disable/delete and Outbox completion in one PostgreSQL UoW.
  Provider unknown becomes `INDETERMINATE`, is not resent and leaves local state unchanged.
- Kept local-only/already-inactive mutations local, but added expected identity/status guards and a
  fail-closed reread so concurrent center registration cannot be overwritten or deleted.
- Added per-source batch occurrences, terminal/partial failure classification and frontend polling
  for every queued operation. Route ownership is now `306 new / 37 legacy` decorators and
  `328 new / 39 legacy` method bindings.

Targeted gate:

- Scoped Ruff/format -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- P9 application -> `89 passed`; disposable PostgreSQL 18 P9 scope -> `41 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`; compileall, `uv lock --check` and
  `git diff --check` -> clean.
- Frontend typecheck/build/API boundary passed; Vitest -> `7 passed`; build retained only the
  existing large-chunk warning. Manifest, `285 paths / 337 operations` OpenAPI and all three secret
  scans are clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-sharing-source-lifecycle-20260830/summary.md`.

Focused review:

- Closed the review-discovered TOCTOU, terminal batch misclassification, alias fallback and overly
  broad center-disable predicate findings with application and PostgreSQL 18 regressions.
- Final transaction and HTTP/UI rereviews found no deterministic P0/P1/P2 in this slice.

Pending:

- Shared Resource re-registration/reporting, center import, virtual effects, credit refresh and
  maintenance routes remain, along with unavailable native tasks and Route/Table/Config ownership.
- Pre-removal inventory, controlled legacy-runtime removal and all P11 NFR/security/SBOM/
  vulnerability/License gates remain pending. P11 stays `DOING`; P12 stays `TODO`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Sharing-Control-Routes

Stage / Batch: P11 / Shared Resource control-route cutover

HEAD / Commit:

- Pre-slice HEAD: `33da01c9` (`refactor(p11): persist shared source lifecycle`).
- Target checkpoint message: `refactor(p11): cut over sharing control routes`.

Implemented:

- Made the append-only local Contribution Ledger authoritative for summary and credit refresh; the
  legacy Shared Center credit snapshot can no longer override either response, and refresh performs
  no external request.
- Replaced the Shared Resource maintenance UI submission with the native
  `POST /api/shared/resources/maintenance/run` route. The route creates or replays a persistent
  `shared-resource-maintenance` Job, freezes `max_attempts=1`, bounds the stable idempotency key and
  performs no provider work in the request thread.
- Preserved `POST /api/shared/resources/center/replenish/prepare` as the exact administrator-only
  Rapid v2 `400` retirement contract, including an explicit OpenAPI no-success exception rather than
  a fictitious `2xx` response.
- Regenerated Manifest, OpenAPI and the TypeScript schema. Route ownership is now `309 new / 34
  legacy` decorators and `331 new / 36 legacy` method bindings; OpenAPI is `288 paths / 340
  operations`.

Targeted gate:

- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- P9 application -> `90 passed`; disposable PostgreSQL 18 P9 scope -> `42 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`.
- Frontend client generation, typecheck, build and API-boundary -> passed; Vitest -> `7 passed`;
  build retained only the existing large-chunk warning.
- Manifest/OpenAPI currentness, three fixture/contract secret scans, scoped compileall,
  `uv lock --check` and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-sharing-control-routes-20260830/summary.md`.

Focused review:

- Reviewed Ledger authority, stale legacy snapshot isolation, permission ordering, Job replay and
  conflict/failure mapping, request-thread provider isolation, frontend occurrence retention and the
  fixed-`400` OpenAPI contract. No deterministic P0/P1/P2 remains in this slice.
- One read-only audit agent modified three scoped files contrary to its instruction. It was stopped;
  provenance was confirmed, retained lines were independently reviewed, and the full targeted gate
  was rerun serially. No unexplained writer or file remains.

Pending:

- Native-only Worker remains `22 executable / 28 unavailable`; 34 route decorators, 36 method
  bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 11 standalone settings
  remain legacy-owned.
- Complete the remaining Shared Resource effects/routes and all unavailable native tasks, reach
  Route/Task/Table/Config/Frontend ownership 100%, then generate the final pre-removal inventory,
  perform controlled legacy removal and run every P11 NFR/security/SBOM/vulnerability/License gate.
  P12 remains `TODO` and Live Acceptance remains `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Source-Registration

Stage / Batch: P11 / durable Shared Resource source registration

HEAD / Commit:

- Pre-slice HEAD: `3a532819` (`refactor(p11): cut over sharing control routes`).
- Target checkpoint message: `refactor(p11): persist shared source registration`.

Implemented:

- Migrated report-center and single/batch reregister routes to a prerequisite state machine: the
  request thread freezes source/files/RAW identity, reserves identifier-only Outboxes and performs
  no provider mutation. RAW remains in `p115_mediainfo_cache` until the Worker validates hash/size
  and crosses the durable checkpoint.
- Added same-UoW source/file/account-bound holder validation at the provider checkpoint, unique-only
  authoritative readback, no-resend recovery, prerequisite replay fencing and atomic local
  confirmation. Source-only fallback preserves the current holder PickCode and original provider.
- Bounded RAW/summary durable data and rejected unsafe URL, absolute/local path, traversal, Bearer
  and token-query values. Frontend waits for terminal state and retains occurrence identity only
  for indeterminate results.
- Regenerated manifests, OpenAPI and TypeScript schema. Route ownership is now `312 new / 31 legacy`
  decorators and `334 new / 33 legacy` method bindings; OpenAPI is `291 paths / 343 operations`.

Targeted gate:

- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- P9 application -> `113 passed`; disposable PostgreSQL 18 P9 scope -> `54 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`.
- Frontend generation, typecheck, build and API boundary passed; Vitest -> `7 passed`; build retained
  only the existing large-chunk warning.
- Manifest/OpenAPI currentness, compileall, `uv lock --check`, three secret scans and
  `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-sharing-source-registration-20260831/summary.md`.

Focused review:

- Closed checkpoint TOCTOU, ambiguous readback, source-only PickCode, RAW/summary bounds, durable
  metadata leakage, batch response and frontend idempotency-key findings.
- Final transaction and HTTP/frontend/OpenAPI rereviews found no deterministic P0/P1/P2. One
  reviewer exceeded its read-only boundary by running expected generators; the main thread
  regenerated serially and reran the complete targeted gate from the final stable worktree.

Pending:

- Native-only Worker remains `22 executable / 28 unavailable`; 31 route decorators, 33 method
  bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 11 standalone settings
  remain legacy-owned until the next inventory proves otherwise.
- Complete remaining Shared Resource effects/routes and all ownership, then pre-removal, controlled
  legacy-runtime removal and the P11 NFR/security/SBOM/vulnerability/License gates. P12 remains
  `TODO`; Live Acceptance remains `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-STRM-Execution

Stage / Batch: P11 / native-only STRM task execution

HEAD / Commit:

- Pre-slice HEAD: `7fbe108b` (`refactor(p11): persist shared source registration`).
- Target checkpoint message: `refactor(p11): enable native strm tasks`.

Implemented:

- Added a typed account-bound `HttpxP115StrmSyncGateway`, including stable OpenAPI/Cookie fallback,
  typed pagination, path lookup and bounded subtitle download on the pinned backend.
- Wired `full-sync-strm` and `monitor-115-life-events` into the native-only Worker graph. Backend
  identity is frozen only after the first authoritative page succeeds; the operation plan cannot
  persist a speculative backend.
- Added a native import sentinel and exclusive STRM creation primitive. Exclusive create uses an
  atomic same-directory link and never replaces a pre-existing or racing user file.
- Native-only Worker ownership is now `24 executable / 26 unavailable`. The remaining unavailable
  keys are unchanged; `play-pool-daily-speedtest` was deliberately not allow-listed because its
  native playback dependency graph is not yet complete.

Targeted gate:

- Scoped Ruff -> clean; scoped Pyright -> `0 errors, 0 warnings, 0 informations`.
- P6 STRM/directory/application/ownership scopes -> `78 passed` and `57 passed`.
- Disposable PostgreSQL 18 STRM plan/cache/replay scope -> `17 passed`.
- Scoped compileall, `uv lock --check` and `git diff --check` -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-native-strm-20260831/summary.md`.

Focused review:

- Verified credential drift, cancellation, persistent plan/replay, fallback pinning, subtitle
  backend consistency, no legacy imports and same-path race behavior. No deterministic P0/P1/P2
  remains in this slice.

Pending:

- Shared Resource center import/virtual effects and 26 unavailable native task keys remain, along
  with Route/Table/Config ownership, pre-removal, controlled removal and the P11 NFR gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Shared-Virtual-Import

Stage / Batch: P11 / Shared Resource virtual import and Rapid transfer

HEAD / Commit:

- Pre-slice HEAD: `2bc6d1c9` (`refactor(p11): enable native strm tasks`).
- Target checkpoint message: `refactor(p11): persist shared virtual imports`.

Implemented:

- Added durable center import, virtual retirement and promotion operations. Request threads only
  freeze identity and reserve work; Worker execution owns exclusive STRM projection, exact Emby
  deletion/readback, compensation and same-UoW terminal continuations.
- Added account/backend/credential/target-bound 115 Rapid v2 transfer with a single provider-write
  checkpoint, persisted phase receipts, readback-only replacement-Worker recovery and deterministic
  Shared Center success/failure reporting.
- Closed terminal delete replay after the virtual row is removed, source-only PickCode persistence,
  runtime target/backend drift and OpenAPI response-gate coverage.
- Frontend now aggregates all batch terminal results, exposes every operation ID and retains the
  idempotency key only for indeterminate results, including non-2xx all-indeterminate batches.
- Regenerated manifests, OpenAPI and TypeScript schema. Route ownership is `315 new / 28 legacy`
  decorators and `337 new / 30 legacy` bindings; OpenAPI is `294 paths / 346 operations`.

Targeted gate:

- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- P9 application -> `127 passed`; disposable PostgreSQL 18 P9 scope -> `63 passed`.
- P1 import/CLI, OpenAPI contract and shared virtual ownership sentinel -> `7 passed`.
- Frontend generation, typecheck, unit, build and API boundary passed; Vitest -> `7 passed`; build
  retained only the existing large-chunk warning.
- Manifest/OpenAPI currentness, compileall, `uv lock --check`, three secret scans and
  `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-shared-virtual-import-20260831/summary.md`.

Focused review:

- Closed runtime identity drift, no-resend/readback, terminal replay, no-files PickCode, batch
  response and frontend operation-handle findings.
- Final Rapid state-machine and transaction/HTTP/frontend/OpenAPI reviews found no deterministic
  P0/P1/P2; both reviewers remained read-only.

Pending:

- Native-only Worker remains `24 executable / 26 unavailable`; 28 route decorators, 30 method
  bindings, 12 tables, 82 dynamic config keys, seven bootstrap keys and 11 standalone settings
  remain legacy-owned.
- Complete all remaining runtime ownership, then pre-removal, controlled legacy removal and the
  full P11 NFR/security/SBOM/vulnerability/License gates. P11 stays `DOING`; P12 stays `TODO`;
  Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-P115-Maintenance-Operations

Stage / Batch: P11 / P115 maintenance routes and native operations

HEAD / Commit:

- Pre-slice HEAD: `5ebac4a0` (`refactor(p11): persist shared virtual imports`).
- Target checkpoint message: `refactor(p11): migrate p115 maintenance operations`.

Implemented:

- Migrated speedtest, mkdir, recycle-bin empty, unrecognized empty, Quick Deploy and MoviePilot
  rename import/export to native FastAPI ownership and bounded processor-free operations.
- Enforced task-key/payload identity, one attempt, stale-worker fail-closed recovery and no terminal
  resume. HTTP timeout paths retain the durable `job_id` and report `indeterminate`.
- Quick Deploy freezes P115/Emby identity, performs authoritative readback without resend, commits
  all local configuration in one UoW, returns only allowlisted config and has finite NDJSON
  deadlines. Stale execution remains an original-Job/manual-reconciliation boundary.
- Regenerated manifests, OpenAPI and TypeScript schema. Route ownership is `322 new / 21 legacy`
  decorators and `344 new / 23 legacy` bindings; OpenAPI is `301 paths / 353 operations`.

Targeted gate:

- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- P115 application/repository/native Worker scope -> `210 passed`; disposable PostgreSQL 18 Job
  and maintenance scope -> `20 passed`.
- P1 import/CLI plus OpenAPI contract -> `5 passed`.
- Frontend generation, typecheck, unit, build and API boundary passed; Vitest -> `15 passed`; build
  retained only the existing large-chunk warning.
- Manifest/OpenAPI currentness, compileall, `uv lock --check`, three secret scans and
  `git diff --check` -> clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-p115-maintenance-20260831/summary.md`.

Focused review:

- Final state-machine/HTTP/frontend/OpenAPI/ownership rereview found no deterministic P0/P1/P2.
- One inherited inventory Subagent exceeded its read-only boundary; provenance was bounded and all
  affected checks were rerun serially from the final stable worktree.

Pending:

- Native-only Worker remains `24 executable / 26 unavailable`; 21 route decorators, 23 method
  bindings, 12 tables and remaining config/frontend ownership still require migration.
- Complete `play-pool-daily-speedtest` and the remaining native graph, then pre-removal, controlled
  legacy removal and the full P11 NFR/security/SBOM/vulnerability/License gates. P11 stays `DOING`;
  P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-Play-Pool-Speedtest

Stage / Batch: P11 / native play-pool daily speedtest

HEAD / Commit:

- Pre-slice HEAD: `3a96baa8` (`refactor(p11): migrate p115 maintenance operations`).
- Target checkpoint message: `refactor(p11): execute native play pool speedtest`.

Implemented:

- Added SQLAlchemy-owned play-pool config, primary-holder source lookup, reward ledger and native
  Rapid/direct-URL/bounded-Range/delete adapters. Native Worker now owns the daily speedtest graph
  without importing legacy `handler`, `routes`, `tasks`, raw-pool config or provider helpers.
- Bound each operation to account/config/credential/source identity and an operation-scoped remote
  filename. Credential fences cover signer lookup and the actual streamed Range read; durable
  receipts allowlist only speed facts and never retain provider messages, URLs, cookies or tokens.
- Enforced no-resend recovery after Rapid, speed and delete checkpoints. Delete readback that still
  finds the file is retryable; exhausted ambiguity remains indeterminate. Unsupported PREPARE and
  RECLAIM currently fail closed before provider I/O and remain a required P11 follow-up.
- Kept play-pool traffic, reward and daily-operation identity on the `Asia/Shanghai` business date.
  Reward or Telegram persistence failure reports a partial result without disabling an account
  whose speedtest already succeeded.
- Native Coordinator schedules only the two processor-free daily tasks. Native-only Worker is now
  `25 executable / 25 unavailable`; Route ownership remains `322 new / 21 legacy` decorators and
  `344 new / 23 legacy` method bindings.

Targeted gate:

- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- Full P6 application -> `783 passed`; full P9 -> `128 passed`; full P4 -> `346 passed` with the
  existing Starlette WSGI deprecation warning.
- Focused P4/P6/P9/architecture gate -> `247 passed` with the same existing warning.
- Disposable PostgreSQL 18 native/existing playback gate -> `27 passed`. The first combined run
  exposed a transient immediate-cleanup timing failure; both isolated scope and the exact combined
  command passed on clean rerun without a code change.
- OpenAPI currentness and P9 sharing fixture contract passed; `git diff --check` is clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-native-play-pool-speedtest-20260831/summary.md`.

Focused review:

- Closed remote same-content deletion, primary-holder binding, fallback ambiguity, streamed
  credential drift, provider-data leakage, delete terminal-state, reward and business-date issues.
- Current batch/OpenAPI/frontend/source-registration delegations were rechecked against the final
  tree: batch `404` is absent, mixed-batch indeterminate results retain their idempotency key, and
  source-only fallback persists the holder PickCode. No deterministic P0/P1/P2 remains.
- One read-only reviewer modified two already-scoped files. The main thread bounded the provenance,
  independently reviewed the retained lines and reran all final gates after the reviewer stopped.

Pending:

- Implement full native PREPARE/RECLAIM recovery and the remaining 25 processor-free task owners.
- Close the 21/23 legacy Route bindings, 12 legacy tables, 82 dynamic config keys, seven bootstrap
  keys, 11 standalone settings and frontend component/API/admin-guard evidence before controlled
  removal and the P11 NFR/security/SBOM/vulnerability/license gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-Playback-Recovery

Stage / Batch: P11 / native interactive playback operation recovery

HEAD / Commit:

- Pre-slice HEAD: `089fca88` (`refactor(p11): execute native play pool speedtest`).
- Target checkpoint message: `refactor(p11): recover native playback operations`.

Implemented:

- Made Native playback own `PREPARE` and `RECLAIM` in API and compatibility Worker wiring, with
  frozen primary/pool credential identity and fresh pinned SDK clients per provider operation.
- Added exact fixed-FID OpenAPI/Cookie readback and checkpoint-after-mutation no-resend for
  PREPARE, RECLAIM, ENSURE_TEMP_DIR and SPEEDTEST.
- Persisted computed PREID before Rapid/COPY mutation and reused it during recovery. Response-lost
  PREPARE may adopt a unique remote match but never claims deletion ownership.
- Required exact boolean `created_remote=true` before any remote delete. Missing, false or malformed
  ownership safely releases locally before provider client creation.
- Fenced historical artifacts with missing provider identity as `INDETERMINATE`; cleanup treats
  artifact revision/lease conflicts as per-item races and continues processing the batch.

Targeted gate:

- Focused P6/P9/P6-architecture gate -> `182 passed`.
- Full P6/P9/P4 application gate -> `1302 passed`, with the existing Starlette WSGI deprecation
  warning.
- Disposable PostgreSQL 18 playback gate -> `31 passed`.
- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- Compileall, `uv lock --check`, Manifest/OpenAPI currentness, three fixture/contract secret scans
  and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-native-playback-recovery-20260831/summary.md`.

Focused review:

- Closed fixed-FID response normalization, COPY rejection/adoption ordering, historical identity
  fencing, phase-missing resend, recovery ownership race, missing-PREID durability, malformed
  ownership deletion and per-artifact cleanup conflict findings.
- Final scoped review found no deterministic P0/P1/P2 in this recovery slice.

Pending:

- Complete native interactive source/direct-URL/streaming ownership and the remaining 25 native-only
  unavailable task keys.
- Close the 21/23 legacy Route bindings, 12 legacy tables, 82 dynamic config keys, seven bootstrap
  keys, 11 standalone settings and Frontend ownership before pre-removal, controlled legacy removal
  and the P11 NFR/security/SBOM/vulnerability/License gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Native-Interactive-Playback

Stage / Batch: P11 / native interactive playback cutover

HEAD / Commit:

- Pre-slice HEAD: `d7ea2481` (`refactor(p11): recover native playback operations`).
- Target checkpoint message: `refactor(p11): cut over interactive playback`.

Implemented:

- Added SQLAlchemy POOL/COPY/VIRTUAL source lookup with primary-holder, asset-array, PickCode/SHA1,
  virtual provenance and ambiguity fencing.
- Added native primary/pool artifact direct URL resolution with frozen credential/account revision
  checks and a bounded streaming transport with Range/header allowlists, redirect revalidation,
  private literal rejection, `trust_env=False`, deterministic close and signed-URL redaction.
- Cut API and both Worker profiles from Legacy playback user/source/config/direct-link wiring to
  SQLAlchemy/native owners. Configured primary OpenAPI/Cookie priority remains authoritative.

Targeted gate:

- Full P6 application -> `837 passed`.
- Interactive direct/source/ownership focus -> `94 passed`.
- P4 Worker outcomes plus P6 architecture -> `121 passed`.
- Disposable PostgreSQL 18 native/existing playback -> `35 passed`.
- Scoped Ruff/format and Pyright, compileall, `uv lock --check`, Manifest/OpenAPI currentness,
  three secret scans and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-native-interactive-playback-20260831/summary.md`.

Focused review:

- No deterministic P0/P1/P2 remains in source selection, provider identity, streaming or active
  wiring after final local review.
- Two read-only reviewers exceeded their no-write boundary during the slice. The main thread
  interrupted them, removed incompatible partial wiring, independently audited retained changes
  and reran all final gates from the stable worktree.

Pending:

- Complete the remaining 25 native-only unavailable task keys and exact 21/23 legacy Route
  bindings, then Table/Config/Frontend ownership, pre-removal, controlled removal and P11 NFR gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

## LIVE

Status: HUMAN_PENDING

Notes:

- Real Emby, 115, MoviePilot, shared center, Telegram, HDHive, production database, production deployment, and stable observation are not authorized by this Goal.
- Local completion must report these checks as pending rather than accepted.

## Handoff

Read first:

- `goal-tasks.md`
- `goal-log.md`
- `full-project-refactoring-execution-plan.md`

Next:

- Resume the 25 unavailable native-only task keys and exact 21/23 legacy Route bindings from the
  latest manifests/runtime call graph. Close Route, Task, Table, Config and Frontend ownership
  before controlled removal. Do not start P12 until P11 ownership, removal and NFR gates are
  complete; keep all Live Acceptance items pending.

### P11-Native-AutoSubscribe

Stage / Batch: P11 / native AutoSubscribe execution

HEAD / Commit:

- Pre-slice HEAD: `fa7ceb37` (`refactor(p11): cut over interactive playback`).
- Target checkpoint message: `refactor(p11): execute native auto subscriptions`.

Implemented:

- Replaced the `auto-subscribe` compatibility callable in both Worker profiles with the
  processor-free `AutoSubscribeService`, durable parent/source claims and MoviePilot execution
  bound to the current Job attempt.
- Added durable Shared Center, HDHive and Telegram source operations with stable occurrence,
  provider/config/credential fences, parent cancellation handling and no-resend recovery.
- Added replay-safe Telegram `enqueue_once`, owner-bound opaque search-result storage and a native
  Coordinator search consumer. Raw links/codes/text do not enter command receipts or Sharing
  operations; virtual-import aliases and serialized sensitive payloads fail closed.
- Fenced AutoSubscription mutations with claim token, Job, attempt, worker and live lease identity.

Targeted gate:

- Scoped Ruff/format over 32 changed Python files and Pyright -> clean (`0 errors`).
- Full P4/P7/P9 application gate -> `836 passed`, with the existing Starlette WSGI deprecation
  warning.
- Disposable PostgreSQL 18 Telegram bridge plus AutoSubscribe lifecycle -> `22 passed`.
- Redis/Celery native-only and compatibility Worker profiles -> `2 passed`, `35 deselected`.
- Manifest currentness, compileall, `uv lock --check`, three secret scans and `git diff --check` ->
  clean. Task ownership is now `26 executable / 24 unavailable`.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-native-auto-subscribe-20260831/summary.md`.

Focused review:

- Final read-only review found no deterministic P0/P1/P2 in native Telegram search consumption,
  sensitive-alias rejection or parent claim/lease fencing.
- Native-only Telegram intentionally consumes only `search` and requires an existing authorized
  session; real Telegram and full legacy/custom-regex parity remain Live `HUMAN_PENDING` evidence.

Pending:

- Complete the remaining 24 native-only unavailable task keys and exact 21/23 legacy Route
  bindings, then Table/Config/Frontend ownership, pre-removal, controlled removal and P11 NFR gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

## Handoff

Read first:

- `goal-tasks.md`
- `goal-log.md`
- `p11-p12-final-completion-handoff.md`

Next:

- Continue the exact 24 unavailable native-only task keys and 21/23 legacy Route bindings from the
  current manifests/runtime call graph; do not begin P12 before P11 ownership, removal and NFR
  closure. Keep all Live Acceptance items pending.

### P11-Native-P8-Media-Tasks

Stage / Batch: P11 / processor-free P8 media task execution

HEAD / Commit:

- Pre-slice HEAD: `9d519de6` (`refactor(p11): execute native auto subscriptions`).
- Target checkpoint message: `refactor(p11): execute native P8 media tasks`.

Implemented:

- Replaced `generate_embeddings`, `refresh-collections`, `execute-auto-tagging-rules` and
  `fill-studio-images` compatibility callables with processor-free services in both Worker
  profiles. Native-only Worker ownership is now `30 executable / 20 unavailable`; the task
  manifest records `28` native/typed owners plus two Worker-owned chain keys.
- Added keyset/CAS embedding persistence and bounded reusable AI clients; authoritative native
  collection reconcile with atomic movie/source publication and hard-ignore fencing; durable
  Auto-tagging and Studio image effects with provider readback, no-resend ambiguity and bounded
  image decoding.
- Serialized Auto-tagging read-modify-write per Emby Item with PostgreSQL advisory locks across
  direct and Outbox execution. Compatibility and native-only profiles each construct one media
  effect executor; claimed effects are reported as indeterminate rather than failed.
- Closed review findings for active collections with NULL Emby identity, future hard-ignore
  revival, concurrent tag loss, empty `ImageTags` fallback, Studio request identity drift, TMDb
  fallback/status classification, lease duration, cancellation checkpoints and effect markers.

Targeted gate:

- Full P8 application -> `209 passed`; Worker outcomes/P8 ownership/static manifests ->
  `79 passed`.
- Disposable PostgreSQL 18 native-owner/CAS/advisory-lock scope -> `5 passed`.
- Redis/Celery compatibility and native-only Worker profiles -> `2 passed`, `37 deselected`.
- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- Manifest currentness, compileall, `uv lock --check`, three secret scans and `git diff --check`
  -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-native-p8-media-tasks-20260901/summary.md`.

Focused review:

- Two findings-first read-only review rounds closed six initial P2 findings and the follow-up
  cross-effect Auto-tagging lost-update finding. Final rereview found no deterministic P0/P1/P2.

Pending:

- Complete the remaining 20 processor-free task owners and exact 21/23 legacy Route bindings,
  then Table/Config/Frontend ownership, pre-removal, controlled removal and every P11
  NFR/security/SBOM/vulnerability/License gate.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Intro-Backfill-Route

Stage / Batch: P11 / native intro backfill HTTP submission

HEAD / Commit:

- Pre-slice HEAD: `ea926357` (`refactor(p11): execute native P8 media tasks`).
- Target checkpoint message: `refactor(p11): cut over intro backfill route`.

Implemented:

- Added a native FastAPI owner for `POST /api/emby/intro-detection/backfill`. The request process
  submits `intro-fingerprint-backfill` with `force=false`, `max_attempts=1`, and the current trace
  identity; it does not scan Emby or resolve an intro/provider gateway.
- Extended the existing processor-free P6 task owner to validate the optional boolean `force`
  parameter while preserving the empty-payload `force=true` catalog behavior.
- Preserved the compatibility HTTP envelopes: accepted submission is `202` with `job_id`; every
  persistent submission failure is `503` with `reason=persistent_submit_failed`.
- Regenerated Route ownership, OpenAPI and the TypeScript client. Route ownership is now
  `323 new / 20 legacy` decorators and `345 new / 22 legacy` bindings; OpenAPI is
  `302 paths / 354 operations`.
- Updated the native Worker process-role assertion to include the two P8 media effect kinds already
  wired by checkpoint `ea926357`.

Targeted gate:

- Full P6 application/unit/architecture gate -> `933 passed`.
- P4 Worker outcomes, OpenAPI and static Manifest gate -> `74 passed`; focused HTTP/ownership
  rerun -> `18 passed`; native process-role gate -> `38 passed` with the existing Starlette WSGI
  deprecation warning.
- Disposable PostgreSQL 18 + Redis/Celery explicit `force=true/false` delivery through native-only
  and compatibility Worker profiles -> `4 passed`, `37 deselected`.
- Frontend client generation, typecheck, API-boundary and production build passed; build retained
  only the existing large-chunk warning.
- Scoped Ruff/format and Pyright -> clean; compileall, `uv lock --check`, Manifest/OpenAPI
  currentness, three fixture/contract secret scans and `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-intro-backfill-route-20260901/summary.md`.

Pending:

- Complete the remaining 20 processor-free task owners and exact 20/22 legacy Route bindings,
  then Table/Config/Frontend ownership, pre-removal, controlled removal and every P11
  NFR/security/SBOM/vulnerability/License gate.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Collection-Activation

Stage / Batch: P11 / durable native Collection activation

HEAD / Commit:

- Pre-slice HEAD: `da95a377` (`refactor(p11): cut over intro backfill route`).
- Target checkpoint message: `refactor(p11): persist collection activation`.

Implemented:

- Added the native `POST /api/emby/collections/activate` callback owner. One PostgreSQL UoW now
  binds the plugin-created BoxSet, reserves `collection.activation.refresh` and writes its
  identifier-only Outbox; provider reads remain Worker-only.
- Added targeted TMDb classification, atomic movie/subscription/collection publication, released
  and future hard-ignore fencing, claim-loss recovery, stale full-refresh snapshot fencing and a
  lossy-downgrade refusal for migration `0025_p11_collection_activation`.
- Replaced the legacy Collection asset gateway in both Worker profiles with the native
  `media_image_cache`/placeholder adapter and aligned its default data root with application
  `local_data` semantics.
- Closed the remaining P8 review findings: active rows with NULL Emby identity are reconciled;
  Auto-tagging uses one authoritative full-item snapshot; P8 inline Media Effects atomically
  reserve and claim before their Outbox is visible, so Auto-tagging/Studio tasks cannot race their
  own delivery or resend after an ambiguous crash.
- Regenerated Route ownership, OpenAPI and the TypeScript client. Route ownership is now
  `324 new / 19 legacy` decorators and `346 new / 21 legacy` bindings; OpenAPI is
  `303 paths / 355 operations`.

Targeted gate:

- Full P8 application/architecture -> `232 passed`; disposable PostgreSQL 18 P8 integration ->
  `155 passed`; focused PostgreSQL 18 platform/collection migration/effects -> `31 passed`.
- Redis/Celery compatibility and native-only Worker profiles -> `2 passed`, `39 deselected`.
- P1 import/runtime, P4 process roles/Worker outcomes and OpenAPI -> `130 passed`. Pytest retained
  only the existing Starlette WSGI deprecation warning.
- Frontend generation, typecheck, `15` unit tests, build and API boundary passed; build retained
  only the existing large-chunk warning.
- Scoped Ruff/format, Pyright (`0 errors, 0 warnings, 0 informations`), compileall,
  `uv lock --check`, Manifest/OpenAPI currentness, contract tooling, three secret scans and
  `git diff --check` -> clean.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-collection-activation-20260901/summary.md`.

Focused review:

- Final transaction/HTTP/OpenAPI/ownership rereview found no deterministic P0/P1/P2 after closing
  missing-TMDb configuration recovery, default asset-root drift and Media Effect first-claim race.

Pending:

- Complete the remaining 20 processor-free task owners and exact 19/21 legacy Route bindings,
  then Table/Config/Frontend ownership, pre-removal, controlled removal and every P11
  NFR/security/SBOM/vulnerability/License gate.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-Provider-Control-Cleanup

Stage / Batch: P11 / native provider control, logical-season share and Cleanup execution

HEAD / Commit:

- Pre-slice HEAD: `514d6039` (`refactor(p11): persist collection activation`).
- Target checkpoint message: `refactor(p11): cut over provider control routes`.

Implemented:

- Added 18 native provider-control route owners for HDHive, Telegram UserBot, Shared Center and
  cloud-download flows, with durable browser/command/operation recovery and opaque persisted state.
- Added replay-safe Telegram search/listener commands, bounded retry and owner-bound secret rows;
  raw links, receive codes and candidate text do not enter durable command or Sharing envelopes.
- Added native logical-season Cookie share transfer with manifest/channel/provider identity fences,
  supported raw 115 request transport, one checkpoint and readback-only recovery.
- Replaced Cleanup execution's legacy callable with explicit native Worker dispatch and
  processor-free local/media/115/Emby adapters, including credential fencing, aligned projection
  updates, Episode→Season→Series offline cascade and shared-source lifecycle convergence.
- Closed batch-reregister contract/key-retention, holder PickCode, hard-ignore, active-source
  filtering, stale best-version and Shared Resource Manager transfer-mode findings.
- Regenerated ownership/OpenAPI/client artifacts. Route ownership is now `342/343` decorators and
  `366/367` method bindings; only native `POST /webhook/emby` remains to close Route ownership.

Targeted gate:

- Full P4/P6/P7/P8/P9 application -> `1943 passed`, with the existing Starlette WSGI deprecation
  warning.
- Disposable PostgreSQL 18 provider/Cleanup gate -> `173 passed`; Cleanup scope -> `49 passed`.
- Full Redis/Celery runtime gate -> `41 passed`.
- Frontend generation, typecheck, API boundary, `26` unit tests and production build passed; build
  retained only the existing large-chunk warning.
- Ruff/format and Pyright (`0 errors, 0 warnings, 0 informations`) passed. Compileall,
  `uv lock --check`, Manifest/OpenAPI currentness, three secret scans and `git diff --check` are
  clean. OpenAPI is `320 paths / 375 operations`.
- Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-provider-control-cleanup-20260901/summary.md`.

Focused review:

- Closed the deterministic `mode=share` UI reachability finding and independently audited the
  reviewer-originated Cleanup changes before retaining them. No deterministic P0/P1/P2 remains in
  this bounded provider-control/Cleanup slice.
- Shared Center report/status uncertainty remains explicit `indeterminate` and never replays a
  confirmed 115 transfer; authoritative external reconciliation remains Live evidence.

Pending:

- Complete the remaining 20 processor-free catalog task owners and `POST /webhook/emby`.
- Complete Telegram title-to-TMDb resolution, completed-pack washing and local pack/episode
  duplicate fencing before removing its legacy consumer.
- Close 12 legacy tables, 82 dynamic config keys, seven bootstrap keys and nine standalone
  settings, then pre-removal, controlled removal and every P11 NFR/security/SBOM/vulnerability/
  License gate.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-MoviePilot-Webhook

Stage / Batch: P11 / native MoviePilot webhook, assistant reconciliation and P115 transfer

HEAD / Commit:

- Pre-slice HEAD: `5dbf76ac` (`refactor(p11): cut over provider control routes`).
- Target checkpoint message: `refactor(p11): cut over moviepilot webhook`.

Implemented:

- Replaced the final active Flask binding, `POST /webhook/emby`, with a FastAPI ingress that keeps
  the legacy envelopes and reserves durable Worker-owned assistant/transfer Media Effects.
- Added native assistant state, torrent, suppression, snapshot and receipt transactions plus strict
  MoviePilot active-download readback. State-digest, timeout-marker and cycle CAS fences prevent a
  stale readback or old effect from overwriting a newer washing/download cycle.
- Added stable restart/timeout Subscription Operations, subscription-ID delete preflight,
  retryable SQL failure handling and explicit terminal failure receipts. Timeout `0` remains the
  compatibility disabled sentinel; timeout convergence no longer depends on provider availability.
- Added native MoviePilot transfer through immutable account/backend/credential/source-bound P115
  plans/jobs, including sidecar, file-info and Episode identity handling.
- Regenerated Route/OpenAPI/client artifacts. All `343` route decorators and `367` method bindings
  now have native owners; OpenAPI is `321 paths / 376 operations`.

Targeted gate:

- Full P7 application -> `405 passed`; disposable PostgreSQL 18 MoviePilot webhook -> `14 passed`.
- P115 organize-plan/ownership plus OpenAPI -> `30 passed, 1 deselected`; the single-head check was
  isolated because the preserved custom-collection draft contains the uncommitted next migration.
- Frontend generation was hash-stable; typecheck, API boundary, `26` unit tests and production build
  passed with only the existing large-chunk warning.
- Scoped Ruff/format and Pyright (`0 errors, 0 warnings, 0 informations`) passed. Compileall,
  `uv lock --check`, Manifest/OpenAPI currentness, three secret scans and `git diff --check` are
  clean.
- Detailed evidence: `/tmp/etk-refactor-artifacts/p11-moviepilot-webhook-20260901/summary.md`.

Focused review:

- Closed provider-unavailable timeout liveness, terminal-operation zombie, duplicate event,
  cycle-supersession, provider/local readback race, unknown-season and pending-state findings.
- Final static rereview found no deterministic P0/P1/P2 in this bounded webhook/transfer slice.

Pending:

- Complete the remaining `20` legacy-compatible task execution owners, including native
  subscribe-assistant maintenance for historical state, then finish Table/Config/Frontend
  ownership, pre-removal, controlled removal and all P11 NFR/security/SBOM/vulnerability/License
  gates.
- P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

Safety boundary:

- No real Emby, 115, TMDb, MoviePilot, Telegram, HDHive, Shared Center, production database, old
  `/config`, external write, push, deployment, release, `web_app.py`, or PyArmor runtime was used.

### P11-People-Metadata

Stage / Batch: P11 / processor-free People and Metadata task owners

HEAD / Commit:

- Pre-slice HEAD: `3d380289` (`refactor(p11): execute native custom collection tasks`).
- Target checkpoint message: `refactor(p11): execute native people and metadata tasks`.

Implemented:

- Added processor-free People task execution for actor translation, alias enrichment, actor
  tracking, duplicate-person merge and ghost-person purge, with bounded Emby/TMDb adapters,
  fresh-reference checks and explicit task outcomes.
- Added processor-free media metadata population/backfill/role translation and screenshot
  backfill owners, preserving file/person projections and scoping offline reconciliation to the
  selected library assets.
- Added cached provider translation clients without importing the legacy `AITranslator`; both
  Worker profiles and the native-only allowlist now route these task keys through the new owners.
- Actor tracking now handles TV cast order and Season metadata, keeps scan-cursor updates
  non-destructive when TMDb returns no usable credits, and resolves person IMDb identity
  conflicts inside the PostgreSQL Unit of Work.
- Regenerated task ownership and retained `.DS_Store` outside the staged set.

Targeted gate:

- P5/P7 application and architecture scope -> `603 passed`, with the existing Starlette WSGI
  deprecation warning.
- New People/Metadata application scope -> `8 passed`.
- Disposable PostgreSQL 18 People/Metadata repositories -> `7 passed`.
- Scoped Ruff/format and Pyright -> clean (`0 errors, 0 warnings, 0 informations`).
- Compileall, `uv lock --check`, task manifest currentness and `git diff --check` -> clean.

Focused review:

- Closed the missing actor-work normalizer, TV order/Season publication, empty-credit destructive
  cleanup, metadata upsert TOCTOU, selected-library offline scope, screenshot Video candidate
  coverage, duplicate-person rewrite duplication and IMDb identity conflict findings.
- No real Emby, TMDb, AI provider, production database or other external write was used.

Pending:

- P11 remains `DOING`: the remaining Watchlist/Resubscribe/Assistant and Covers/destructive task
  owners, Table/Config/Frontend ownership closure, pre-removal, controlled removal and all P11
  NFR/security/SBOM/vulnerability/License gates are still required.
- P12 remains `TODO`; Live Acceptance remains `HUMAN_PENDING`.

### P11-Subscription-Review-20260902

Stage / Batch: P11 / findings-first review of the uncommitted Watchlist, Resubscribe and Assistant WIP

Status: `DOING` (no checkpoint)

Evidence:

- Current WIP adds `src/etk/domains/subscription/task_execution.py` (2,647 lines) and wires six task
  keys into the native-only Worker before the committed task manifest is regenerated.
- Scoped application tests -> `57 passed`; P7 application/architecture -> `443 passed, 1 warning`;
  P7 PG18 WIP scope -> `3 passed`; scoped Ruff/format/Pyright -> clean.
- `uv run python scripts/refactor/check_manifests.py` correctly reports the committed task manifest as
  stale; no manifest or ownership promotion was performed.

Findings / blocker:

- Watchlist execution currently publishes SQL/TMDb state and an Emby refresh effect, but does not own
  the legacy MoviePilot, 115, shared-center, metadata/image and per-series continuation branches
  (`src/etk/domains/subscription/task_execution.py:256-343`).
- Resubscribe execution explicitly returns `partial` for delete/HDHive rows and otherwise stages only
  MoviePilot replacement operations (`:370-416`), while the legacy task still has local file/NFO/
  subtitle/empty-directory/DB cleanup, 115 deletion, Emby refresh and HDHive branches.
- Assistant maintenance has phase ledger plumbing, but timeout handling treats a missing provider as
  an empty readback, history reconciliation reads only the failed page, subscription sync persists
  the full provider mapping, and download/snapshot phases do not implement the legacy registration,
  progress, repair and authoritative readback behavior (`:1602-2100`). These are ownership/correctness
  gaps, not lint failures.

Decision:

- The reliability primitives remain necessary; the over-designed part is the migration shape: a
  single large subscription execution file plus premature native-only/manifest promotion. Per the
  handoff stop rule, unresolved P1 ownership findings prevent a checkpoint, manifest regeneration,
  controlled removal and P12 entry. No files were reset, cleaned, stashed or deleted.

Next:

- Do not add more task keys or process the cover WIP until the subscription owner boundary is either
  completed with processor-free external effects/readback evidence or explicitly re-scoped by the
  user. Then run one focused transaction/PG18 review, regenerate manifests, and checkpoint.

External actions: no push, deployment, release, real external write, or Live Acceptance action.

### P11-Delay-Audit-20260902

Stage / Batch: P11 / scope and design audit before the next native owner

Evidence:

- Current `task-ownership.yaml` has `50` task keys: `36` native-use-case, `4` typed-provider-adapter, and `8` legacy-compatibility-callable.
- The remaining eight keys are three watchlist tasks, two resubscribe tasks, one subscribe-assistant task, and two cover tasks. They still resolve to legacy compatibility gateways in the committed manifest.
- Route ownership is already `343/343` decorators and `367/367` bindings, but table/config ownership is not closed (`12` legacy tables, `82` legacy dynamic keys, `7` legacy bootstrap keys, and `9` legacy standalone settings).
- The uncommitted subscription draft is `2,748` lines and combines Watchlist, Resubscribe, and Subscribe Assistant. It is not wired into either Worker profile and its own review records partial external effects for each family.
- Recent P11 commits added `3,584`, `5,099`, and `6,215` lines respectively for People/Metadata, custom collections, and MoviePilot webhook/transfer; this is migration-surface growth, not evidence that the remaining owners are complete.

Decision:

- Reliability primitives (PostgreSQL UoW, claim/lease, Outbox, idempotency, checkpoint/no-resend, authoritative readback, `INDETERMINATE`, and replay/CAS fences) remain required by the P11 contract.
- The over-designed part is the migration shape: one large cross-domain draft and premature Worker/manifest promotion. Partial owners must not be promoted or used to justify removal.
- The next slice is intentionally one bounded local owner, `update-resubscribe-cache`, with no provider write. The incomplete Watchlist/Resubscribe/Assistant draft and cover draft remain unpromoted and are not a checkpoint.

Pending:

- Complete and verify the bounded cache owner, then continue with the remaining task families. P11 stays `DOING`; P12 stays `TODO`; Live Acceptance stays `HUMAN_PENDING`.

### P11-Subscription-Safety-20260902

Stage / Batch: P11 / subscription maintenance safety hardening (still uncommitted)

Implemented:

- Snapshot cleanup now preserves active claims and only removes expired unclaimed rows.
- Full-washing timeout treats a missing MoviePilot provider as no authoritative readback and leaves
  the snapshot unchanged for a later retry.
- Transfer-history reconciliation reads failed plus recent pages, deduplicates records and keeps
  stable id/fingerprint identities.
- MoviePilot subscription sync persists only a bounded allowlisted projection and merges it with local
  state instead of overwriting local timeout/download markers.
- Download reconciliation registers newly observed typed active-download identities while retaining
  the existing empty-response safety fence.

Targeted gate:

- Subscription application -> `12 passed`.
- Subscription PG18 WIP scope -> `3 passed`.
- Scoped Ruff/format/Pyright -> clean.

Pending:

- These changes close deterministic safety defects but do not yet establish full Watchlist,
  Resubscribe or Assistant task ownership. Do not regenerate the task manifest or create a checkpoint
  until the missing processor-free external effects, readback, continuation and recovery behavior is
  implemented and reviewed.

### P11-Subscription-Scope-Reset-20260902

Stage / Batch: P11 / prevent premature subscription owner promotion

Implemented:

- Removed the uncommitted six-key subscription entries from the Worker native-only allowlist and
  processor-free compatibility dispatch. Compatibility Worker startup again uses the established
  `LegacySubscribeAssistantMaintenanceGateway`; the native subscription draft remains untracked and
  is not constructed by either Worker profile.
- Restored the native-chain regression assertion to use `process-watchlist`, so an unfinished task
  cannot be hidden behind an unrelated cover key. The task exporter no longer advertises the six
  subscription keys as native owners.

Evidence:

- `uv run pytest -q -p no:cacheprovider verification/application/p4/test_worker_outcomes.py` -> `53 passed`.
- `uv run pytest -q -p no:cacheprovider verification/application/p7 verification/architecture/p7` -> `445 passed, 1 warning`.
- `uv run pytest -q -p no:cacheprovider verification/application/p9` -> `156 passed`.
- `uv run python scripts/refactor/check_manifests.py` -> `manifest_check=clean`.
- `git diff --check` and scoped Ruff remain clean.

Decision / blocker:

- The reliability primitives remain in scope, but a 2,748-line draft cannot be promoted while its
  Watchlist, Resubscribe and Assistant behavior is only partial. The committed task manifest still
  truthfully contains eight legacy-compatible keys (six subscription and two cover); ownership,
  removal, P11 NFR and P12 remain pending. No reset, clean, stash, delete, push, deployment, release
  or real external write was performed.

### P11-Subscription-Outcome-Safety-20260903

Stage / Batch: P11 / subscription draft safety correction (still uncommitted)

Implemented:

- Native Subscribe Assistant maintenance now distinguishes a missing MoviePilot readback from an authoritative empty result and leaves the timeout snapshot untouched when readback is unavailable.
- Typed MoviePilot write results are normalized by explicit outcome (`confirmed`/`rejected`/`indeterminate`) instead of Python object truthiness; indeterminate writes raise a durable maintenance error and cannot be checkpointed as success.

Targeted gate:

- `verification/application/p7/test_subscription_task_execution.py` -> `13 passed`.
- Scoped Ruff, format and `py_compile` for the draft module/test -> clean.
- `git diff --check` -> clean.

Pending:

- This safety correction does not promote the draft or change the committed ownership manifest. Full processor-free Watchlist, Resubscribe library and Subscribe Assistant behavior, provider readback/continuation, Worker wiring and PG18 recovery evidence remain required before a checkpoint.

### P11-TASK-001-Subscribe-Assistant-20260908

TASK / HEAD:

- `TASK-001 subscribe-assistant-maintenance`; pre-checkpoint HEAD `d650f0e9`.

Implemented:

- Completed the processor-free Subscribe Assistant owner across timeout, history, subscription sync,
  no-download, download-monitor and snapshot-verify phases; added typed/paged MoviePilot readback,
  authoritative downloader snapshots, stable durable effect occurrences, suppression and claim fences.
- Both Worker profiles now use the native owner without resolving a legacy processor; task ownership
  was promoted only after the application, PG18, Worker and architecture gates passed.

Gate:

- Assistant/MoviePilot/Worker application scope -> `156 passed`; full P7 application/architecture ->
  `462 passed`; disposable PostgreSQL 18 assistant plus existing runtime regression -> `24 passed`.
- Scoped Ruff/format, `py_compile`, Pyright (`0 errors, 0 warnings, 0 informations`), manifest
  currentness and `git diff --check` are clean. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-subscribe-assistant-20260908/summary.md`.

Pending / Blocker / Next:

- No TASK-001 P0/P1/P2 blocker remains. P11 stays `DOING`, P12 stays `TODO`, Live stays
  `HUMAN_PENDING`; continue directly with `TASK-002` and keep the unfinished Watchlist/Resubscribe
  and cover drafts outside this checkpoint.

### P11-TASK-002-Watchlist-20260908

TASK / HEAD:

- `TASK-002 add-all-series-to-watchlist / process-watchlist / refresh_completed_series`;
  pre-checkpoint HEAD `920a6832`.

Implemented:

- Added the processor-free Watchlist owner, atomic Series/Season/Episode projection, bounded
  TMDb/Emby reads, durable MoviePilot/115/Shared Center/Emby effects, targeted intro follow-up,
  stable replay identities, and both Worker profile bindings.
- Closed focused findings for full-washing delete suppression/local state, 115 already-classified
  no-op behavior, legacy-compatible Shared Center display payload, and completed-series effect scope;
  promoted all three keys in the ownership exporter and generated manifest.

Gate:

- Application/Worker/Sharing targeted gate -> `622 passed, 1 warning`; disposable PostgreSQL 18
  plus migration targeted gate -> `27 passed`; scoped Ruff/format and Pyright
  (`0 errors, 0 warnings, 0 informations`) passed.
- Manifest currentness, Alembic single head `0027_p11_watchlist_display`, and `git diff --check`
  passed. Detailed evidence: `/tmp/etk-refactor-artifacts/p11-watchlist-20260908/summary.md`.

Pending:

- TASK-003 `resubscribe-library`, TASK-004 covers, TASK-005 ownership closure, TASK-006 controlled
  removal/P11 full gate, and TASK-007 P12 disposable Greenfield remain.

Blocker:

- None in TASK-002 scope.

Next:

- Create the isolated TASK-002 local checkpoint, then continue directly with TASK-003. P11 remains
  `DOING`, P12 remains `TODO`, and Live remains `HUMAN_PENDING`.

### P11-TASK-003-Resubscribe-20260908

TASK / HEAD:

- `TASK-003 resubscribe-library`; pre-checkpoint HEAD `32093300`.

Implemented:

- Added the processor-free resubscribe owner with per-item MoviePilot, delete and HDHive dispatch;
  reused the existing durable replacement workflow, Cleanup child jobs, quota ledger and P9 Sharing
  operation executor instead of reviving direct legacy deletes or adding a new effect framework.
- Both Worker profiles now construct the owner. Native Cleanup staging carries explicit parent
  Job/Attempt/Worker identity without importing `task_manager`; disabled or drifted rule/index
  identities fail closed before durable effects. Ownership was promoted only after the focused gate.

Gate:

- TASK-003 application/Cleanup/resubscribe scope -> `162 passed`; full P7 application, architecture
  and process roles -> `524 passed, 1 warning`; disposable PG18 resubscribe/Cleanup scope ->
  `57 passed`; Worker/manifest contracts -> `67 passed`.
- Scoped Ruff/format, Pyright (`0 errors, 0 warnings, 0 informations`), compileall, `uv lock --check`,
  manifest currentness and `git diff --check` passed. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-resubscribe-20260908/summary.md`.

Pending:

- TASK-004 covers, TASK-005 ownership closure, TASK-006 controlled removal/full P11 gate and
  TASK-007 disposable Greenfield remain. Project-wide Pyright and cleanup-scan PG18 drift are
  assigned to those owning slices; TASK-003 scoped gates are clean.

Blocker:

- None in TASK-003 scope.

Next:

- Create the isolated TASK-003 local checkpoint, then continue directly with TASK-004. P11 remains
  `DOING`, P12 remains `TODO`, and Live remains `HUMAN_PENDING`.

### P11-TASK-004-Covers-20260908

TASK / HEAD:

- `TASK-004 generate-all-covers / generate-custom-collection-covers`; pre-checkpoint HEAD `8a64cac2`.

Implemented:

- Added the processor-free cover owner to both Worker profiles, preserving existing target/source,
  style, configuration, badge, local artifact and Emby upload behavior.
- Added stable per-Job effects, pre-render durable recovery, frozen payload replay, bounded streamed
  image/font reads and explicit Worker effect tracking; removed both legacy fallback mappings and
  promoted ownership only after validation.

Gate:

- Cover/Worker/media-effect/P8 architecture scope -> `179 passed, 1 warning`; disposable PostgreSQL
  18 cover/media-effect scope -> `18 passed`.
- Scoped Ruff/format, Pyright (`0 errors, 0 warnings, 0 informations`), compileall, `uv lock --check`,
  manifest currentness and `git diff --check` passed. Detailed evidence:
  `/tmp/etk-refactor-artifacts/p11-covers-20260908/summary.md`.

Pending / Blocker / Next:

- No TASK-004 P0/P1/P2 blocker remains. P11 stays `DOING`, P12 stays `TODO`, Live stays
  `HUMAN_PENDING`; create the isolated TASK-004 checkpoint, then continue directly with TASK-005
  Table/Config and active-ownership closure.

### P11-TASK-005-Ownership-20260908

TASK / HEAD:

- `TASK-005 Table/Config and active ownership closure`; pre-checkpoint HEAD `6c4b725c`.

Implemented:

- Closed Route/Task/Table/Config/Frontend active ownership. Tables are `37 new / 2
  retired-approved`; bootstrap/dynamic config are `7/96 new`; standalone settings are `43 new / 11
  retired-approved`; no legacy or unresolved dynamic setting remains.
- Added processor-free PostgreSQL translation-cache ownership and installed it in both native media
  and People tasks. Coordinator runtime context now uses typed Settings plus one native app-settings
  snapshot and has no dynamic old-runtime imports.
- Kept the two P9 credit tables, five playback aggregate settings and legacy stream-feature mapping
  only as evidence-backed approved replacements; no schema or public contract changed.

Gate:

- Application/architecture/contracts/Settings/role/translation scope -> `291 passed, 1 warning`;
  disposable PostgreSQL 18 table/config/repository scope -> `94 passed`.
- Scoped Ruff/format and Pyright (`0 errors, 0 warnings, 0 informations`) passed. Compileall,
  `uv lock --check`, manifest currentness, three secret scans and `git diff --check` passed. Detailed
  evidence: `/tmp/etk-refactor-artifacts/p11-table-config-20260908/summary.md`.

Pending / Blocker / Next:

- No TASK-005 P0/P1/P2 blocker remains. P11 stays `DOING`, P12 stays `TODO`, Live stays
  `HUMAN_PENDING`; create the isolated TASK-005 checkpoint, then continue directly with TASK-006
  pre-removal evidence, controlled active-runtime exit and the single P11 full gate.

### P11-TASK-006-Pre-Removal-20260908

TASK / HEAD:

- `TASK-006 pre-removal evidence`; capture HEAD `0dd8d7cb`.

Implemented:

- Reconfirmed Route/Task/Table/Config/Frontend ownership closure and saved the old active entry,
  native replacement, import/reference scan, image inventory and prior-gate index before removal.
- Built `etk-refactor:pre-removal-0dd8d7cb`; the image excludes top-level `web_app.py`, PyArmor,
  `task_manager.py` and `scheduler_manager.py`, but still contains the environment-selectable API,
  Worker, Coordinator and runtime-supervisor compatibility paths recorded for the removal slice.

Gate:

- Manifest currentness and `git diff --check` passed; the pre-removal image build and content probes
  passed. Detailed evidence: `/tmp/etk-refactor-artifacts/p11-pre-removal-20260908/summary.md`.

Pending / Blocker / Next:

- No pre-removal blocker remains. P11 stays `DOING`, P12 stays `TODO`, Live stays
  `HUMAN_PENDING`; create the isolated evidence checkpoint, then remove only active compatibility
  reachability and run the affected gates before the single P11 full gate.

### P11-TASK-006-Controlled-Removal-20260909

TASK / HEAD:

- `TASK-006 controlled active-runtime removal and P11 full gate`; pre-checkpoint HEAD `ff629da6`.

Implemented:

- Removed active API/Worker/Coordinator reachability to the WSGI fallback and legacy
  Processor/TaskManager/SchedulerManager paths while retaining historical source outside the active
  runtime. The final image excludes `web_app.py`, PyArmor and the compatibility runtime modules.
- Reconfirmed all seven final task keys as processor-free native owners: Subscribe Assistant,
  Watchlist (three keys), Resubscribe and Covers (two keys). Table/Config ownership remains `37 new / 2
  retired-approved`, bootstrap/dynamic config `7/96 new`, and standalone settings `43 new / 11
  retired-approved`.
- Added targeted behavioral coverage for native identity, scheduled maintenance, media/image/cast,
  115 upload/projection/organize/delete, review reprocessing and naming contracts. No production
  external provider or protected legacy entry was executed.

Gate:

- Complete non-integration coverage run -> `2644 passed`; disposable PostgreSQL 18 integration ->
  `756 passed`; separately instrumented disposable Redis/Celery subprocess gate -> `37 passed`.
- Final coverage uses the conservative full `domains + processes` changed-code scope: changed lines
  `3313/3464 = 95.64%`; changed branches `1040/1152 = 90.28%`. Bridge, contribution point,
  deletion, idempotency and authentication contracts remain included in the passing suites.
- Full Ruff/format -> clean (`777 files`); Pyright -> `0 errors, 0 warnings, 0 informations`;
  compileall, `uv lock --check`, manifest/OpenAPI currentness, active ownership (`6 passed`) and
  `git diff --check` passed.
- Frontend typecheck, `26` unit tests, build and `8` E2E tests passed; docs install/build passed.
  Final Linux arm64 image `etk-refactor:p11-task006-20260908` is
  `sha256:b42317c3237d109550bd83297e92ef2a917b90528c0876bb8fbca8716025629e`
  (`332082236` bytes) and passed exclusion/import probes.
- NFR performance passed: health p95 `0.660083ms`, database APIs at most `2.135833ms`, Job creation
  p95 `2.437375ms`, raw DB comparisons within `110%`, and zero running Jobs after the benchmark;
  reliability/observability -> `28 passed`. Reachable High/Critical dependency findings are zero;
  retained advisories, SBOMs and license inventories are recorded without hiding residual risk.
  Detailed evidence: `/tmp/etk-refactor-artifacts/p11-task006-20260908/`.

Pending / Blocker / Next:

- No P11 P0/P1/P2 blocker remains; P11 is `DONE`. Create the isolated TASK-006 local checkpoint,
  then run TASK-007's three disposable Greenfield rounds. P12 remains `TODO`; Live remains
  `HUMAN_PENDING`. No push, deployment, release or real external write was performed.

### P12-TASK-007-Greenfield-20260910

TASK / HEAD:

- `TASK-007 disposable Greenfield create/rebuild/failure-recovery`; source and pre-checkpoint HEAD
  `cc367594`.

Implemented:

- Added a guarded P12 harness and focused guard tests. The harness requires new prefixed absolute
  roots, an isolated `etk-p12-*` Project, an explicit local `sha256:` image, loopback ports, exact
  services/mounts/volumes/network labels, a fake Docker socket and ownership revalidation before
  every `down --volumes`.
- The disposable topology uses PostgreSQL 18, Redis, fresh named volumes and the P11 image
  `sha256:b42317c3237d109550bd83297e92ef2a917b90528c0876bb8fbca8716025629e`; Compose runs with
  `--no-build --pull never`, Coordinator schedules disabled and no external credentials.
- A generated local administrator fixture authorizes protected API probes without real setup or
  external identity. Its signed cookie is never persisted, its database row is removed, and each
  final invariant snapshot proves the fixture count returned to zero.

Gate:

- Harness guard scope -> `18 passed`; scoped Ruff/format and Pyright
  (`0 errors, 0 warnings, 0 informations`) passed. Actual Compose v5 `!override` resolution passed.
- Evidence Project `etk-p12-20260910-cc367594-j` completed first empty create, second empty rebuild,
  injected `migrate` exit `42`, and clean failure recovery. All three successful states reported
  PostgreSQL `18.6` (`180006`), Alembic `0027_p11_watchlist_display`, `78` public tables, `50` task
  keys and the seven final P11 owners; both PG18 schema/query tests passed in every state. Live
  read-only configuration, review/media, subscription, actor-subscription and sharing projections
  all returned authoritative empty results.
- In every successful state, a paused Worker retained a queued `sync-115-directory-tree` Job,
  unpause completed it through native `P6P115TaskRouter`, and idempotent replay returned the same
  Job ID. Final snapshots contain one succeeded Job, one idempotency record, one processed Outbox,
  zero active Jobs and zero media/quota/credit invariant violations.
- Every successful state, the injected-failure state and the final harness exit have empty
  container/network/volume inventories. Final evidence:
  `/tmp/etk-refactor-artifacts/p12-greenfield-20260910-j/`; manifest SHA-256
  `8618506d1a2e127ea03dda64bfec41d511c7a8933ad77c2753f74f3ffe224b71`.

Pending / Blocker / Next:

- No P12 P0/P1/P2 blocker remains; P12 is `DONE`; local checkpoint `32d980ce` records the harness,
  guard tests and tracker state. Live external-system acceptance remains `HUMAN_PENDING`. No push,
  deployment, release or real external write was performed.

### DIRECT-REQ-001-Full-Access-20260910

TASK / HEAD:

- Remove the current native ETK Pro purchase/activation system and license gates; pre-checkpoint HEAD
  `971fe7b3`.

Implemented:

- Replaced the General Settings commercial card with a static fully-unlocked state and removed Pro
  pricing, purchase, activation, renewal, transfer and marketing UI/API bindings. Removed Pro labels
  from Virtual Library, TG channel monitoring and the Telegram quick menu.
- Retired the three native Pro APIs, remote authorization gateway and daily status operation. HTTP
  and WebSocket Proxy no longer accept or enforce a license state; normal login/admin permissions,
  feature enable switches and third-party Shared Center/HDHive entitlements remain unchanged.
- Fixed the native configuration writer exposed by the local image rebuild: identity and system
  configuration now share one serialized PostgreSQL-backed writer, reject non-persistent bootstrap
  updates and preserve committed configuration across API restart. Identity projection conflicts now
  honor repository CAS rejection and report that service authorization was already saved.

Gate:

- Current backend/application regression scope -> `125 passed`; ownership/static contracts -> `14
  passed`. Scoped Ruff/format and Pyright (`0 errors, 0 warnings, 0 informations`), manifest currentness,
  OpenAPI currentness and `git diff --check` passed.
- Frontend typecheck, `26` unit tests, API-boundary check and production build passed; the build has
  only the existing Vite large-chunk warning. Browser validation showed `Emby Toolkit 完整功能版`,
  `全部功能已解锁，无需激活码`, `全部解锁` and `虚拟库`, with no upgrade action.
- Final local arm64 image `etk-refactor:local-full-access-final-971fe7b3` is
  `sha256:0b67b2db921702f5df4433f0f013c232f52ca135a2ccd7ee5fdd93764527cef7`
  (`327115557` bytes). All native services are healthy, migrate exited `0`, runtime OpenAPI is
  `318 paths / 373 operations`, and the three Pro routes are absent. A local-only fixture proved
  configuration survives API restart; the fixture was removed and the original four standalone
  app-setting rows were preserved.

Pending / Blocker / Next:

- No DIRECT-REQ-001 P0/P1/P2 blocker remains. The prior Emby authorization was never present in the
  mounted config volume or `dynamic_app_config`, so it cannot be reconstructed; the local page is
  restored to `/setup` and requires one human re-authorization. Live remains `HUMAN_PENDING`. No
  push, deployment, release or real external write was performed.

### DIRECT-REQ-002-Retired-External-Resources-And-Web-Quick-Tasks-20260911

TASK / HEAD:

- Retire TG UserBot channel listening, HDHive and the Shared Resource Center; expose the configured
  Telegram quick tasks in the Web task center and document their behavior. Pre-checkpoint HEAD
  `cbb4ed6a`; the earlier native full-access checkpoint remains unchanged.

Implemented:

- Removed the retired provider-control and sharing routers from the native app, their UI routes,
  settings cards, cloud-search controls and generated client endpoints. Coordinator no longer starts
  a UserBot listener, consumes channel commands, schedules P9 sharing tasks or coordinates sharing
  operations. MoviePilot is the only selectable subscription source; historical HDHive washing rows
  are skipped without provider access.
- Kept the frozen 50-key compatibility registry and historical tables/configuration, while exposing
  only 46 active tasks. New submissions for the four retired P9 keys are rejected before Job creation;
  historical Jobs finish as `SKIPPED / feature_retired`, task chains filter the keys, old shared-intro
  effects skip, and cleanup retains historical shared-source records without invoking their lifecycle.
- Kept Telegram Bot notifications, Chat ID binding and administrator quick-task control. `/search`,
  `/find`, `/hdhive` and old resource callbacks now return a retirement notice; task commands and
  `/all_tasks` are limited to the configured menu and exclude retired tasks and `scan-organize-115`.
- Added an always-visible “网页与 Telegram 快捷任务” card to the task center. Its configured tasks
  reuse the existing `POST /api/tasks/run`, global busy state and four existing incremental/full-mode
  choices. Empty menu configuration follows the existing six-task Telegram fallback. Added one mocked
  E2E submission assertion, without contacting the local backend or executing a real task.
- Added `docs/zh/guide/quick-tasks.md` and updated public navigation and affected documentation. The
  guide records administrator permissions, `202`, Job states, Web `409`, Telegram queueing, the four
  dual-mode tasks, the six defaults, `system-auto-update=SKIPPED`, MoviePilot ownership and the retained
  local 115 `302`/STRM/virtual-library boundary.

Gate:

- Full application -> `2572 passed, 1 warning`; architecture -> `108 passed`; focused PostgreSQL 18
  integration -> `114 passed`. Changed Python Ruff, `handler/telegram.py` compile, Pyright (`0 errors,
  0 warnings, 0 informations`), OpenAPI and manifest currentness, and `git diff --check` passed.
- OpenAPI is `260 paths / 309 operations`; retired Shared Resource Center, HDHive, TG UserBot and cloud
  prefixes are absent, while both documented `302` responses and `/api/p115/play/*` remain.
- Frontend API generation, typecheck, `15` unit tests, API-boundary check, production build and `8` E2E
  tests passed. The final E2E includes a mocked Web quick-task click and exact incremental payload;
  no task or external provider was invoked. Docs build passed; builds retain only the existing large
  chunk warning.
- Read-only browser evidence showed six Web quick-task buttons on desktop and no horizontal overflow
  at `390px`; no “立即执行” button was clicked against the real local service. The already configured
  Emby endpoint also answered read-only `GET /emby/System/Info` with HTTP `200` and version `4.9.3.0`;
  no URL, token or credential was recorded.

Review / retained boundary:

- Low-level P9 provider/delivery classes, the frozen task definitions and historical operation rows
  remain for decoding, audit and data preservation. No public route, schedule, available-task entry,
  accepted new Job or Worker dispatch reaches their external mutation methods; the retained sharing
  operation Celery entry only acknowledges the retired work and never starts the provider runtime.
  Deleting the historical graph, tables, configuration or migrations is intentionally outside this
  request.
- The existing `/api/tasks/run` guard order remains administrator -> global busy -> readiness -> body;
  therefore a globally busy server may return `409` before the task-specific retired `410`, but it
  cannot create a retired Job. Custom Telegram menu entries keep their existing task parameter
  semantics; Web mirrors that configured list and the server remains the validation authority rather
  than adding a second hard-coded task taxonomy.

Pending / Blocker / Next:

- No Local P0/P1/P2 blocker remains under the requested retirement boundary. `DIRECT-REQ-002` is
  `DONE`; P11 and P12 Local remain `DONE`; Live remains `HUMAN_PENDING`. Create the isolated local
  checkpoint `refactor: retire external resource features`. No push, deployment, release or real
  external write was performed.

### Current-Context-Reconciliation-20260912

Request / scope:

- Reconcile `docs/ai-context/` with the actual native mainline. Documentation only; no application,
  Compose, credential, database or plugin changes in this documentation pass.
- Git baseline is `main@2e09b04f2ac7` with pre-existing uncommitted deployment/auth/direct-network
  changes. Those changes and the existing node_modules platform differences were preserved.

Corrected:

- Replaced the old Flask/PyArmor/PG16 entrypoint and single-container hot-update instructions in
  the short/long contexts with native FastAPI, PG18, Celery/Redis and split-role entrypoints.
- Documented current frontend tooling, 50 frozen versus 46 active task keys, approved Pro-gate
  removal, retired Shared Resource Center/HDHive/UserBot listening, and retained Telegram Bot/MP.
- Recorded service-specific direct/proxy routing, masked Emby Token fallback, Proxy/Gateway
  deployment, standard 115 302 scope, current Bridge installation limits and offline-vs-live evidence.
- Marked the original plan and three old handoffs as historical contracts; their stale HEAD,
  dirty lists, remaining-task counts and destructive Greenfield commands are not today's runbook.
  Historical results/checklists are preserved; no Local or LIVE stage was silently advanced.

Deployment evidence from this conversation (not a new deployment in this pass):

- Mac-built linux/amd64 image `10.8.65-local.20260912-proxy8097` was loaded on the authorized Debian
  target. A fresh read-only check in this pass confirmed API, Worker, Coordinator, Control Plane,
  Proxy and Gateway were all healthy on that version.
- Earlier in this conversation, 5257 session/configured-user Views and 8097 Emby home, four Views,
  standard 115 HTTPS 302, Range and WebSocket were checked. This documentation pass did not replay
  those business probes or claim a complete external-system acceptance run.
- The earlier deployment preserved PostgreSQL/Redis container identities and existing volumes,
  config contents, old image and directory archives. Future updates continue via Mac build/export
  and Debian load; DockerHub release, automatic updater adoption and data cleanup remain separate.
- A fresh read-only check found Emby 4.10.0.40 in host networking, without the Bridge loaded and
  without the opt-in label on the running container. The earlier Compose-only label edit was not
  a container recreation. The installer still requires host-network target discovery adaptation.
- Separate plugin-fork verification in this conversation: upstream v1.3.63 and fork source were
  equivalent; build against the actual 4.10.0.40 assemblies had 0 errors and 6 dependency-conflict
  warning groups. Published DLL loaded 153 types, resolved 8,073 IL references without failure,
  and installed/uninstalled all 8 Harmony hook groups in an isolated linux/amd64 process. No
  production plugin installation, business mutation or plugin-coexistence acceptance was performed.

Validation / remaining boundary:

- Current-context source-path check: 49 path references resolved; added local Markdown links and
  code-fence pairing checked; referenced frontend scripts exist. `git diff --check` passed for this
  directory. VitePress docs build passed with the existing large-chunk warning.
- No full backend/frontend business test suite was rerun for a documentation-only edit. Earlier
  stage counts remain dated evidence, not fresh results. No commit, push, release or deployment.
- P11/P12 Local remain `DONE`; P9 remains `LOCAL_DONE`; complete LIVE remains `HUMAN_PENDING`.

### Bridge-Install-And-Delivery-Policy-20260912

用户纠正与授权：

- Mac 构建 linux/amd64 镜像、导出并传 Debian 加载，仅为临时开发验证操作；此前日志中
  “Future updates continue via Mac build/export and Debian load”不应理解为长期交付规则。
  功能正常、版本稳定后发布到 DockerHub，由 Debian 拉取部署；本轮没有发布镜像。
- 用户明确要求现在安装插件。本次仅安装已验证的 Bridge、注册 ETK Origin，并重建一次
  已确认的 Emby 容器应用插件和已有 opt-in label；没有升级 Emby 镜像。

安装与回读证据：

- 安装前 Bridge 不存在，无活动播放 session。已备份原编排、存在的插件配置和 Emby
  配置目录，并记录旧镜像 ID、挂载和插件缺席状态；这不是完整媒体数据库备份。
- 安装上游 v1.3.63 的已验证 DLL，传输后 SHA-256 与验证制品一致：
  `a080d965cb32eaef283c6d0e27668eccf97a052b4bef8d4f4d6cd2b1a8f2f9a7`。
- 原镜像 `--pull never --no-deps --no-build --force-recreate` 重建后，Emby 为
  `running`、restart count 为 0，镜像 ID 未变、host 网络保留，全部挂载与备份逐项一致。
  运行容器 `com.embytoolkit.control-plane.plugin-install=true` 已生效。
- Emby `/Plugins` 回读 `ETK MediaInfo Bridge 1.3.63.0`，更新计划任务
  `ETKMediaInfoBridgeUpdate` 为 Idle；未手动执行更新或修改其计划。
- `/ETKMediaInfo/Origin` 返回 `Configured=true`；真实启动日志确认 8 组 Harmony hook
  激活、事件 relay 启动和中文索引重建完成。Emby 容器可直连 ETK health，管理端 health
  正常，8097 Web 入口 HTTP 200。
- 安装后的 4 个媒体库相关类型配置列表均未包含 `ETK Metadata` / `ETK Images`。
  保留原有 Provider、NFO、实时监控设置；没有重新授权或调用广泛改写库策略的默认配置流程。

边界与回退：

- host 网络自动安装器识别缺口仍存在；本次为明确目标的授权手工安装，不代表修复安装器。
- 回退依据为本次远端安装备份：原先没有 DLL，可将新 DLL 移到插件加载目录之外的备份
  位置，再重启 Emby；配置恢复需核对安装后变化，不能无条件覆盖后续用户配置。
  具体私有备份路径保留在本轮运维回执，不复制到项目文档。
- 完整元数据、图片、事件、删除、播放和其他插件共存业务未验收，LIVE 保持
  `HUMAN_PENDING`。没有 commit、push、GitHub Release 或 DockerHub 发布。
- 本轮文档 `git diff --check -- docs/ai-context` 与 `npm run docs:build` 通过；
  VitePress 仅有现存 large-chunk warning，没有为文档更正重跑全部业务测试。

### Manual-Reprocess-And-Logs-Deployment-20260912

- 用户授权修复旧媒体重新处理问题并部署到 Debian；同时交付此前未部署的实时/历史日志修复。
- 根因：搜索结果来自 Emby，但重新处理只允许 `failed_log` 中已有条目；同步保存了 TMDb/Emby
  ID，却未保存插件取元数据所需的路径关联。实时日志错误地把对象 join 成字符串，历史页面仍读旧文件。
- 修复：单项提交在待复核记录缺席时核实真实 Emby ID/类型；不新增伪造的待复核记录。
  同步及手动重新处理合并路径资产而不覆盖已有资产，手动处理完成后使用持久效果提交根条目刷新。
  刷新不递归、不请求替换图片；普通同步仍不等于全库自动回填，也未增加无 TMDb ID 的自动识别。
- 日志修复保留原接口和管理员权限，历史按根任务及其后代读取数据库快照，显示失败摘要；
  最近 100 个根任务、每任务最多保留 200 条的限制已在界面注明，不宣称完整日志归档。
- 本地验证：P5、复核 HTTP 与日志定向测试 263 项通过；隔离 PostgreSQL 18 集成测试 7 项通过，
  包含路径恢复、资产保留和同操作刷新不重复提交；前端单元测试 17 项及日志浏览器回归 1 项通过。
  类型检查、Ruff 和差异检查通过，linux/amd64 镜像导入烟测通过；前端构建保留既有 large-chunk warning。
- Mac 构建镜像 `etk-refactor:10.8.65-local.20260912-reprocess-logs`，镜像归档传输前后 SHA-256 为
  `85515f09f085b1e1c36992ddaaa67c88cba8878f36d9354fd921729c9754dfe7`。
  Debian 保留唯一镜像标签，同时更新 Compose 使用的本地标签；旧镜像另存回滚标签。
- 更新前确认无活动任务，备份编排、环境文件、配置、PostgreSQL 自定义格式 dump 和目标 Emby 条目。
  只重建六个 ETK 应用角色；数据库/Redis 容器身份未变，挂载路径、卷及读写权限未变，无 schema 迁移。
- 真实单项 canary：搜索结果提交返回 202，任务 succeeded；目标路径元数据 API 从 404 变为 200，
  Emby 回读到 237 字简介、年份及类型。历史列表包含该任务，HTML 显示任务名称与成功状态。
  API health 正常、8097 Web 返回 200，六个应用角色 healthy。
- 没有重建 Emby、全库处理、移动媒体文件、推送代码、GitHub 发版或 DockerHub 发布。
  季集详细补刮、无 ID 自动识别、`refresh.request` 事件接线与全套播放/长期稳定性验收仍未完成；
  LIVE 保持 HUMAN_PENDING。私有备份位置见本轮运维回执。

### Season-Episode-Reprocess-Deployment-20260912

- 用户指出根条目回填不等于季集补刮，授权继续修复并部署。此前根条目 canary 的验收范围过窄，
  本轮不沿用其 succeeded 作为分集成功证据。
- 重新处理按所选整剧/季/集范围取真实 Emby 子项，先保存匹配的季集身份和元数据，再逐项持久刷新；
  名称、非空简介及 TMDb ID 回读一致才记为确认。逐项进度和失败原因写入任务日志，partial 不再合并成成功。
- 无根 TMDb ID 时，仅采用唯一的同名同年候选；无明确年份、歧义或身份冲突要求人工确认，不猜搜索第一项。
  此自动识别由本地回归和隔离 PostgreSQL 流程验证，未移除真实媒体现有 ID 来制造线上测试。
- 本地验证：279 项定向测试通过；19 项隔离 PostgreSQL 集成测试通过，新增身份冲突保护后定向 5 项复跑通过；
  Pyright、Ruff、差异检查和 linux/amd64 镜像导入烟测通过。未修改前端业务代码，镜像内前端构建通过。
- Mac 构建并传输 `etk-refactor:10.8.65-local.20260912-season-episode`，归档 SHA-256：
  `4fba94dd5aa130d9f5b79f3d5c37f8191a71fabdc955ee352c27133653a6519d`。
  Debian 六个应用角色健康；数据库/Redis 身份、应用挂载路径和读写权限未变，无 schema 迁移。
- 真实剧集 canary：选择 39 项，确认 38 项（1 个 Series、2 个 Season、35 个 Episode）；
  唯一失败为 S02E00 在 TMDb 中无对应季集编号，未改写或自动映射到特别篇，整体报告 partial。
  S02E01 的 Emby 显示标题、114 字简介、2023 年和独立分集 TMDb ID 已回读确认；Bridge 分集接口返回 200。
- 仅保留上一份日常部署备份及上一版镜像；更早备份、无运行容器引用的旧镜像和本轮传输包清理，
  已退出成功且仅引用更早镜像的一次性 migrate 容器移除，不删除卷。初始旧项目归档按用户要求保留。
- 没有重建 Emby、移动/重命名媒体文件、全库处理、Git 推送、GitHub Release 或 DockerHub 发布。
  特别篇编号需人工确认，插件 refresh.request 自动处理与全套长期 Live 验收仍未完成。

### Metadata-P115-AI-Local-Fixes-20260912

- 用户授权修复刮削、115 容量未知及 AI 测试导入失败；本轮仅本地实现验证，没有部署、重刮真实媒体或修改凭据。
- 演员保存头像及聚合角色，Bridge 查询在人物详情缓存缺失时使用媒体演员名单，已有缓存姓名仍优先；季集导演保留头像，分集合并常驻及客串。分级、IMDb、关键词和类型中文映射接通，已有分级不被空结果抹掉。
- 分集剧照同时输出 Primary，旧仅 thumb_path 的缓存也可回退为 Primary；仅对缺主图的 Episode 启用非替换图片刷新。任务回读校验补充人物、头像、分级、IMDb 与主图，失败不冒充全成功。
- 115 状态仅对明确失效码执行一次续期，先核对原账号身份再按旧 Token 比较保存，拒绝覆盖并发新授权；续期请求不盲重试、115 直连。Cookie 有效不再掩盖 OpenAPI 失败，前端区分缺容量与真实零空间。
- AI 测试移除对 ai_translator/database 的依赖，直接使用现有 HTTP 客户端支持 OpenAI 兼容、智谱及 Gemini；错误分类且不回显上游错误正文。AI 测试/模型列表进入线程池，局域网、国内及自定义服务默认直连，仅已知受限域名使用已配置代理。
- 本地验证：346 项后端定向测试、21 项隔离 PostgreSQL 18 测试通过；后续分级合并调整后相关 7 项再次通过。前端 19 项单测、typecheck、build 通过；Ruff、Pyright 通过。复用已有 linux/amd64 镜像并只读挂载新 src、禁网验证 AI 原生调用，确认镜像不存在 database 包时测试仍成功。
- 用户填写的是 TMDb ID，无需改为 TVDB ID。没有扩展插件 TVDB/其他职务协议，也没有发版、推送或部署；线上演员、主图、115 续期和真实 AI 服务调用仍待部署后验收。

### Detailed-Logs-And-Filesystem-Monitor-Local-20260912

- 用户明确要求不部署，修复日志详细度、历史日志逐任务拆分以及 MP 本地下载后的自动刮削。
- 只读现场证据：指定资源已硬链接到实际 Emby/监控共同配置的 MoviePilot 库路径，并非源目录和目标目录不同导致；Emby 已有剧集及 E01，E02 当时尚未入库。监控持久事件为 0，MP 持久通知为 0，ETK 的 MP 连接配置未设置。以当前剧名及 2026 年只读查询 TMDb 可唯一匹配 294636；已有 E01 的 Emby 按路径查询返回精确 1 项。
- 根因：原生 Coordinator 有消费/稳定性/派发链，但没有启动真实文件 Observer。新增 Leader-owned Watchdog 入口，随开关/目录变化更新，关闭或丢失 Leader 时停止，与 cron 总开关独立；不启动全库补扫。新分集首次识别父剧时准备必要父剧/季元数据，不处理兄弟集。
- 历史页改为 app.log 合并入口；原 app.log.task-* 是虚拟任务快照，不再在列表逐项显示，旧链接保留。增加跨进程加锁的运行日志文件，10 MiB 轮转、1 份归档；任务快照仍保留最近 100 个根任务，每任务最多 200 条，不能宣称无限或完整任务归档。
- 日志区分元数据/演职人员获取、图片地址准备、实际图片传输/中断、Emby 回读；记录 MP 通知接收、忽略原因及持久入队，文件监控事件、稳定后探测、绑定、自动刮削提交与重试/拒绝。未收到的历史通知不补造。
- 验证：352 项定向回归通过；额外运行日志接线及重新处理检查 21 项通过（与前述有重叠）；22 项隔离 PostgreSQL 集成通过，统一历史查询字段调整后 1 项复跑通过。多进程日志合并、单归档轮转和脱敏有测试；Linux/amd64 镜像禁网真实 Observer 创建文件事件检查通过，前端 build/typecheck、Ruff、Pyright、diff check 通过。
- 不部署、不变更线上目录/媒体库/MP 配置，不重刮真实资源。现有漏项仍需部署后查漏或定向处理；完整自动入库及 MP 通知送达的线上验收未完成。上一批刮削/115/AI 修复继续保留于工作树。

### Monitor-Logs-Debian-Deployment-20260912

- 用户随后明确授权部署，保留由用户重新整理资源进行业务验收。Mac 构建并传输 `etk-refactor:10.8.65-local.20260912-monitor-logs.1`，传输两端归档 SHA-256 一致：`627f49ba22cb897d15baf3562ab5a709b3bb0430e9c3234912cf880ce491a5c6`；运行镜像 ID：`sha256:4195e7f99af4565264c9b9a0823f3e7c8afb910e894f297f237b613a6af49f29`。没有 Git 推送、DockerHub 或 GitHub 发布。
- 首次上线检查暴露进程日志接线遗漏：Coordinator CLI 未初始化日志、Celery 未接入共享文件 handler。补齐 Coordinator 初始化及 Celery setup_logging 信号；任务日志存储列表隐藏内部轮转锁文件。60 项定向回归、Ruff、Pyright、diff check 通过后重新在 Mac 构建传输更新，没有容器内热补丁。
- 六个应用角色均为目标镜像且 healthy；迁移成功退出，schema 保持 `0027_p11_watchlist_display`。DB/Redis 容器 ID/卷保持不变；应用挂载结构比较一致（初次文本比较仅因顺序不同失败）。API health、页面、JS 均 HTTP 200，JS MIME 正确；Gateway 根路径正常返回 302。
- 现场共享日志确认三个配置根目录的 Watchdog 启动；统一历史列表仅 `app.log`，实际运行日志与任务快照合并读取成功。未制造测试媒体、触发全库扫描、主动补刮或修改 MP/Emby 设置；MP 通知送达及完整新增文件刮削等待用户操作，不能标为完整 Live DONE。
- 另外记录：部分 `emby.event.user.*` 播放事件缺少 Worker profile 处理器，包含部署前积压；未在此次日志/监听部署中扩大修复。切换期间旧 API 有一次 Response content shorter than Content-Length，最终实例页面及资源请求正常；完整播放验收未重跑。
- 备份仅保留此次升级前的一份配置/编排/环境/PostgreSQL 回滚点及上一版 season-episode 镜像；定向删除更早普通备份、无引用旧镜像、中间构建镜像和已导入远端传输包。初始旧项目归档继续单独保留，未清理其他服务。

### Playback-Events-And-Readable-Logs-Local-20260912

- 用户要求修复播放事件批量报错及日志可读性；本轮未要求再次部署，保持本地改动，未改线上状态或发送真实 Telegram 通知。
- Worker 缺少用户事件 delivery 注册；补齐原生端口接线，复用既有片头检测 gateway、115 release_for_stop、Postgres 代理租约及 Telegram Bot adapter。未启用通知、缺少 Bot/接收目标时明确跳过；清理缺少可靠会话身份时不降级为整用户清理。Telegram 部分发送后不重放已发送消息，可重试异常最多 12 次后终止自动重试。
- 已退役虚拟导入不再生成新播放后推进任务；积压事件保留可消费处理器，明确确认 feature_retired，不恢复旧 routes/webhook 或删除历史账本。
- 复现 BaseHTTPMiddleware 在流式客户端断连时发送空的最终 body，从而触发 Response content shorter than Content-Length；配置同步改为直接透传 ASGI receive/send。保留同步失败 503、线程池及健康接口豁免，正常 206/Content-Range/Content-Length 透传通过，不靠删除长度头掩盖异常。
- 日志 view 原先直接 reverse 每一行，导致 traceback 倒置；改为完整记录分组倒序，合并任务日志排序也保留记录边界。HTML 展示完整时间、级别和来源，详情折叠；未知格式中性显示，不标红为错误。增加级别筛选及成功健康检查/状态轮询的可逆隐藏，原始日志继续保留；前端补刷新按钮与 iframe 标题。
- 验证：定向应用回归通过，14 项隔离 PostgreSQL 集成通过（含 5 类积压播放事件进入 terminal 且重放不重复副作用）；Ruff、Pyright、前端 typecheck/build 通过。另有重试耗尽及完整 Range 流测试通过。CUA 浏览器使用合成日志验证默认视图、堆栈展开、ERROR 筛选和健康检查显示，桌面与 390px 窄屏检查通过；已关闭预览服务并恢复视口。没有对外发送通知、删除真实播放资源或部署。

### Playback-Events-Debian-Deployment-20260912

- 用户授权部署。Mac 构建 `linux/amd64` 镜像 `etk-refactor:10.8.65-local.20260912-playback-events`，禁网原生 imports 检查通过；归档两端 SHA-256 一致：`4c644a851eb2d3813afb588f0d5143a5678e792ee7e42d46f95d06a0eacfa117`。六角色均运行镜像 `sha256:09cc2423a29a2e864440e665d966c872b06459d46a5b4f501f6b2dfb164aa074`。
- 升级前备份配置、编排、环境和 PostgreSQL，迁移成功退出且 schema 仍为 `0027_p11_watchlist_display`；DB/Redis ID 与卷、应用媒体挂载结构均不变。六角色 healthy，API/页面/JS 返回 200 且 JS MIME 正确，Gateway 根路径 302。
- 上线后既有 93 条 pending 播放事件全部 confirmed：51 条 Telegram 为 notification_disabled，6 条片头检测为 disabled，12 条虚拟导入为 feature_retired；12 条播放清理与 12 条并发释放完成。没有人工改账本或发送测试通知；自动处理结果不能宣称真实 Telegram 已验收。
- 格式化历史服务读取成功，包含级别筛选/健康检查过滤及堆栈详情；Coordinator 新日志确认监控已启动。新容器日志检查中 MediaEffectExecutionUnavailable、Response content shorter than Content-Length、raised unexpected、Exception in ASGI 计数均为 0；这仅代表检查时段，历史错误记录保留。
- 仅保留升级前 monitor-logs.1 镜像及一份普通回滚备份；定向清理更早 season-episode 镜像/普通备份和远端已导入传输包。初始旧项目归档保留，没有 DockerHub/GitHub 发布，也未主动扫描或补刮真实媒体。
