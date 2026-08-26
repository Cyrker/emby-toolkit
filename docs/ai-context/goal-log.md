# Goal Log

Source: `full-project-refactoring-execution-plan.md`
Task status truth: `goal-tasks.md`

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

Status: DOING

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

- Continue with `P7-HTTP/STATE`, then `P7-RUNTIME`. Keep all Live Acceptance items pending.
