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

- Continue P1 from the P0 stage commit. Keep all Live Acceptance items pending.
