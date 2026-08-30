# P11-P12 最终完成交接

> 用途：从当前 Mac 工作树继续完成 P11 与 P12 Local Code Complete。本文只记录当前增量事实和执行顺序；架构、完整 DoD 与验证矩阵仍以 `full-project-refactoring-execution-plan.md` 和 `p7-p12-mac-goal-repair-handoff.md` 为准。
> 当前日期：2026-08-31。
> 当前状态：P8 `DONE`、P9 `LOCAL_DONE`、P10 `DONE`、P11 `DOING`、P12 `TODO`、Live Acceptance `HUMAN_PENDING`。

## 1. 强制读取顺序

写入前完整读取：

1. `AGENTS.md`
2. `docs/ai-context/short-term-context.md`
3. `docs/ai-context/long-term-knowledge-base.md`
4. `docs/ai-context/full-project-refactoring-execution-plan.md` 的 P11/P12 与退出门槛
5. `docs/ai-context/goal-tasks.md`
6. `docs/ai-context/goal-log.md` 从 `P11-Shared-Source-Lifecycle` 到最新 `Handoff`
7. `docs/ai-context/p7-p12-mac-goal-repair-handoff.md` 的 P11/P12、验证、Git 和停止条件
8. 本文

`goal-tasks.md` 是状态事实源；`goal-log.md` 只追加紧凑证据。原执行计划和本文不作为状态 tracker 修改。

## 2. Git 与 writer 基线

仓库：

```text
/Users/cyrker/Desktop/project/emby-toolkit-refactor-mainline
```

分支与已提交 HEAD：

```text
codex/refactor-mainline
3a5328199a0afb629f28ffce985897b1eb2e67cd
refactor(p11): cut over sharing control routes
```

开始时只读运行：

```bash
git status --short --branch
git log -12 --oneline
git diff --check
git rev-parse HEAD
git diff --stat
```

预期工作树为一个已知、未提交的 P11 Shared Resource source-registration 批次：22 个 tracked 修改、2 个 untracked 文件（本文和 `source_registration.py`）。不得 reset、clean、stash、删除或拆成未知批次。若出现本文无法解释的额外修改，停止写入并报告。

主线程必须是唯一 writer。只允许边界明确的只读 reviewer；任何 reviewer 不得修改文件、Git 或 Goal tracker。

## 3. 当前 dirty 批次

目标路由：

```text
POST /api/shared/resources/shares/{source_id}/report-center
POST /api/shared/resources/shares/{source_id}/reregister
POST /api/shared/resources/shares/reregister-batch
```

已知 dirty 文件：

```text
docs/ai-context/p11-p12-final-completion-handoff.md  # 本交接文件，已知 untracked
frontend/src/api/generated/schema.ts
frontend/src/components/SharedResourceManagerPage.vue
scripts/refactor/export_openapi.py
src/etk/domains/sharing/execution.py
src/etk/domains/sharing/operation_delivery.py
src/etk/domains/sharing/projection.py
src/etk/domains/sharing/repository.py
src/etk/domains/sharing/router.py
src/etk/domains/sharing/service.py
src/etk/domains/sharing/source_registration.py   # 已知 untracked
src/etk/integrations/shared_center.py
src/etk/processes/worker.py
verification/application/p9/test_external_adapters.py
verification/application/p9/test_sharing_http.py
verification/application/p9/test_sharing_operation_process.py
verification/contracts/baseline-summary.json
verification/contracts/http/p4-openapi.json
verification/contracts/ownership-overrides.json
verification/contracts/route-ownership.yaml
verification/contracts/source-inventory.json
verification/fixtures/http/p9-sharing.json
verification/integration/p9/test_sharing_ledger_postgres18.py
verification/integration/p9/test_sharing_projection_postgres18.py
```

### 3.1 目标状态机

```text
HTTP request
  -> freeze local source/files/RAW identity
  -> reserve blocked source-registration operation
  -> reserve RAW-upload prerequisite + identifier-only Outbox
  -> Worker loads and uploads RAW after durable checkpoint
  -> RAW confirmed/readback confirmed
  -> same UoW releases dependent Outbox
  -> Worker registers source
  -> immediate receipt or authoritative readback
  -> same UoW commits local source/files + operation terminal + Outbox terminal
```

必须保持：请求线程无外部 mutation；checkpoint 后 no-resend；dependent 在 RAW confirmed 前无 Outbox；本地 continuation 与 terminal 状态同事务；保留原 `source_provider`；缺 PickCode 按 virtual/non-transferable skip；account-bound 115 holder 和 `XXX` 分级 fail closed；UI 等待 terminal，不能把 `202` 当成功。

## 4. 当前 reviewer findings 与半成品修复

事务 reviewer 确认三个问题。修复已开始落盘，但被中断，**尚未运行任何测试、静态检查或复审**。

### 4.1 Confirmed prerequisite replay

问题：prerequisite 已 `CONFIRMED` 后，相同 idempotency replay 会再次 release 已进入 `CLAIMED/REQUESTING/AWAITING_READBACK/RETRYABLE` 的 dependent，可能返回 500。

当前补丁：`_release_dependent_operation()` 对 terminal 或非 `READY`/已有 claim 的 dependent replay no-op，仅为仍可释放的 `READY` dependent stage Outbox。

必须补测：dependent 为 `REQUESTING`、`AWAITING_READBACK`、`RETRYABLE` 的同 key replay；不得改变状态、重发 provider 或错误复活 dead Outbox。

### 4.2 115 sign-holder TOCTOU

问题：原 state hash 未覆盖 `p115_filesystem_cache` 的真实 account-bound holder；staging 后 holder 消失仍可能发送 provider mutation。

当前补丁：

- projection 按 account ID + SHA1 解析当前可签名 PickCode；
- snapshot/continuation 冻结 holder account、PickCode、SHA1；
- repository 新增 `validate_prewrite()`；
- executor 在 provider checkpoint 前复核 source/files/holder；
- 确定性 drift 预写入 `REJECTED`，临时 DB 错误才 retryable；
- confirmation continuation 再次 `FOR UPDATE` 复核。

必须补测：staging 后删除或修改 holder/source/file，provider request/checkpoint 均为 0；checkpoint 后恢复只能 readback，不能 resend；confirmation 仍保持 local projection、operation、Outbox 原子提交。

### 4.3 RAW durable payload

问题：完整 RAW 曾写入 `sharing_operations.request_json`，没有上限，普通 metadata value 可能携带 URL、临时路径或凭据。

当前补丁：

- `freeze_shared_raw_ffprobe()` 使用 8 MiB 上限并生成 SHA256/size；
- operation 只保存 `raw_reference={sha1,sha256,size_bytes}` 和有界 summary；
- Worker checkpoint 前从本地 `p115_mediainfo_cache` 加载 RAW，重新 sanitize 并验证 size/hash；
- `raw_reference` 在 provider call 前移除；
- URL、临时路径、Bearer/token query 形态值 fail closed。

当前明确未完成：

- `src/etk/processes/worker.py` 已引用 `_load_sharing_raw_ffprobe`，但 helper 尚未定义；
- Fake/application/PG18 fixtures 尚未更新 holder/ref shape；
- size/hash/cache-missing/sensitive-value/no-full-RAW 测试尚未补齐；
- HTTP/frontend/OpenAPI reviewer 上次被中断，修复后必须重新执行。

helper 应最小实现：调用现有 `SqlAlchemyP115MediaInfoRepository.find_by_sha1()`，只返回 detached `record.raw` Mapping；cache/RAW 缺失返回 `None`；不得读取旧 `/config`、调用 115 或访问网络。

## 5. 当前批次验证与 checkpoint

reviewer 修复前曾有 `verification/application/p9: 96 passed`、`verification/integration/p9: 45 passed`，以及 frontend、Manifest、OpenAPI、Ruff、Pyright 成功证据；这些都早于当前补丁，不能作为完成证据。

修复后运行一次完整 targeted gate：

1. scoped Ruff/format/Pyright；
2. P9 application；
3. `ETK_RUN_POSTGRES_INTEGRATION=1` 的 P9 integration；
4. P1 import/CLI + OpenAPI contract；
5. frontend `api:generate`、typecheck、unit、build、API boundary；
6. Manifest/OpenAPI regenerate + currentness；
7. compileall、`uv lock --check`、三组 secret scan、`git diff --check`；
8. findings-first transaction review；
9. findings-first HTTP/frontend/OpenAPI/ownership review。

详细证据：

```text
/tmp/etk-refactor-artifacts/p11-sharing-source-registration-20260831/summary.md
```

更新紧凑 `goal-log.md`，`goal-tasks.md` 保持 P11 `DOING`，然后创建：

```text
refactor(p11): persist shared source registration
```

不 amend、不 push。提交后不得停止，继续剩余 P11。

## 6. 剩余 P11

当前 committed 事实约为：native-only Worker `22 executable / 28 unavailable`，legacy tables `12`，dynamic config keys `82`，bootstrap keys `7`，standalone settings `11`。当前 dirty 三条 route 收口后 route legacy 预期约 `31 decorators / 33 bindings`；必须重新生成，不能手填计数。

已知下一组 Shared Resource 残余至少包括：

```text
POST /api/shared/resources/center/import
DELETE /api/shared/resources/virtual-imports/{virtual_id}
POST /api/shared/resources/virtual-imports/{virtual_id}/promote
```

之后从最新 Route/Task/Table/Config/Frontend manifests 和真实 runtime call graph 生成精确清单，完成所有 P115、HDHive、Telegram、cloud subscription、Webhook、Collection、Intro 及其他残余。不要重做已提交 P7-P10/P11 owner，除非当前测试或 review 给出确定失败证据。

ownership 只能在真实可执行 owner 存在后提升：wrapper/Manifest 修改不是证据；native-only Worker 不得 `NativeTaskExecutionUnavailable`、compatibility fallback、动态 legacy import 或依赖旧 Processor/manager/store；table/config 必须没有 active legacy reader/writer；frontend 入口必须有可执行 native API。

剩余 ownership 尽量压缩为 2–3 个完整批次。每批一次 targeted gate、artifact、focused review 和独立 checkpoint；阶段末全套门禁只运行一次。

### 6.1 Pre-removal

只有 Route/Task/Table/Config/Frontend ownership 全部 100%、native-only startup/Worker/Coordinator 与 import sentinels 通过、无 P0/P1/P2 后，才能生成：

- legacy 删除候选和逐项 native replacement；
- active import/reference 扫描；
- Docker image 内容清单；
- `web_app.py` 保留与 image 排除证据；
- PyArmor runtime image 排除证据；
- rollback/checkpoint 边界。

### 6.2 Controlled removal + NFR

Pre-removal 通过后一次受控移除 active WSGI fallback、active Flask/gevent/psycopg2/requests/extensions 依赖、旧 Processor/TaskManager/SchedulerManager 和启动建表调用。不得删除、修改、导入、执行、解密或反编译 `web_app.py`/`pyarmor_runtime`。

随后按原计划只运行一次阶段末完整 Ruff、Pyright、pytest、PostgreSQL 18、Redis/Celery、frontend、Manifest/OpenAPI、secret scan、Compose config、coverage、性能、可靠性、资源、SBOM、漏洞和 License 门禁，并完成高风险事务 focused review、P11 integration review 和 image/import review。

只有 ownership 100%、removal 完成、全部必需门禁通过且无 P0/P1/P2，才把 P11 标为 `DONE`。

## 7. P12 Local Greenfield

P11 `DONE` 后才进入。只执行 disposable local：

```text
create -> migrate -> verify -> capture evidence -> destroy
       -> rebuild from empty -> verify -> failure-rebuild drill
```

不执行真实目标部署、外部端口开放、真实 Scheduler/Watchdog/Webhook 或真实外部 canary；这些保持 Live `HUMAN_PENDING`。

优先扩展现有 harness；不存在时只新增最小工具。任何 destroy 前必须证明：目标是 allow-root 下的 absolute realpath；Compose Project 使用隔离 Greenfield 前缀；resolved Compose 的 bind mounts 全在 disposable root；所有 container/network/volume labels 属于当前 project；无旧 `/config`、生产别名、生产容器或未知 Volume。任一归属无法证明必须拒绝 destroy。

使用 PostgreSQL 18、Redis、新空 config/数据库、锁定镜像、Stub/Fake/脱敏 fixture；关闭真实外部写入。保存两次空状态创建和一次失败后清空重建的证据，包括 image digest、PostgreSQL version、Alembic revision、resolved Compose、labels/volumes、配置摘要、日志和测试报告。

证据目录：

```text
/tmp/etk-refactor-artifacts/p12-greenfield-20260831/
```

完成后更新 P12 `DONE`，Live 仍为 `HUMAN_PENDING`，创建独立 P12 checkpoint。健康、HTTP 200、container healthy 或页面打开不能单独作为完成证据。

## 8. 全程边界

禁止：`git reset --hard`、`git clean`、stash、amend、force push、push、deploy、release、未知文件删除、并发 writer、真实 Emby/115/TMDb/MoviePilot/Telegram/HDHive/Shared Center/生产数据库、旧 `/config`、真实外部写入，以及对 `web_app.py`/PyArmor 的修改、导入、执行、删除、解密或反编译。

允许：本地源码修改、本地 checkpoint、disposable PostgreSQL 18、Redis/Celery、归属 guard 保护的 disposable Greenfield、Fake/Stub/loopback 和脱敏 fixtures。

只有 P11、P12 Local 全部完成且没有必需工作剩余时才将 Active Goal 标记 complete；Live Acceptance 始终单独保持 `HUMAN_PENDING`。最终报告必须列出最终 HEAD、全部新 checkpoint、实际通过命令和结果、未运行项及原因、最终 ownership、P11 NFR/security 结果、P12 三组证据、剩余 P0/P1/P2，并明确未 push/deploy/release/real external write。
