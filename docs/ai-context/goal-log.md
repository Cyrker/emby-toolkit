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

- Continue P2 from the P1 stage commit. Keep all Live Acceptance items pending.
