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

- Continue P4 from the P3 stage commit. Keep all Live Acceptance items pending.
