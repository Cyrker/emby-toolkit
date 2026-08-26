# ETK P7-P12 Mac Goal 修复与压缩执行交接

> 文档用途：在 Mac 上恢复 `13c6d32e` 之后的全项目重构 Goal，先修复已确认的 P7 缺陷，再以压缩批次完成 P7-P12。
> 审查基线：`origin/main@13c6d32ee546c103838130a0869a4b613f593b4f`
> 当前阶段：P0-P6 `DONE`，P7 `DOING`，P8-P12 `TODO`，Live Acceptance `HUMAN_PENDING`
> 风险等级：高。涉及订阅额度、远端副作用、删除、持久任务恢复、贡献点和 Greenfield 销毁重建。

## 1. 交接结论

恢复 Goal 前必须先完成两项代码修复和一次状态文档收口：

1. 修复 MoviePilot 写入与取消并发时错误 terminal `CANCELLED`、释放 quota、停止 readback recovery 的 P1 缺陷。
2. 修复 P7 HTTP fixture 在 Windows 默认 GBK 环境下无法读取 UTF-8 JSON 的问题。
3. 把 `13c6d32e` 的实际 P7 checkpoint 和 P7-P12 压缩执行规则写入现有 Goal 文档。

不重新实现或重新探索已经通过的 Actor、Watchlist 和 subscription operation runtime；只有真实测试失败或代码审查发现确定缺陷时才回到这些模块。

**Handoff path：** `bugfix-backed` 的 P1 修复，随后继续既有 migration plan。

**Dev start recommendation：** `handoff-full`。TASK-001 使用 `dev-bugfix`；P7-P12 阶段执行使用 `dev-spec-driven-implementation`；P10 使用 `dev-frontend-ui-implementation`；阶段末使用 `dev-pr-review` 和对应验证门禁。Skill 由主线程执行，Subagent 不承担业务取舍，也不并发修改同一 checkout。

## 2. 事实基线

### 2.1 已确认状态

- 远端目前只有 `main`，HEAD 为 `13c6d32e wip(p7): checkpoint subscription migration`。
- P7 checkpoint 包含 69 个文件，约 `+16,848/-214`。
- checkpoint 已包含 subscription runtime、Actor/Watchlist routes、Cleanup/Resubscribe foundations、PostgreSQL operation state、process wiring 及 P7 验证资产。
- P7 仍为 `DOING`，不能标记为 `DONE`。
- 本次 Windows 只读复核结果：
  - P7 application/architecture：设置 UTF-8 模式后 `154 passed`。
  - P7 scoped Ruff：通过。
  - P7 scoped Pyright：`0 errors, 0 warnings`。
  - OpenAPI currentness：通过。
  - Manifest currentness：8 个 committed manifests stale，属于未收口 checkpoint，不能作为 P7 完成证据。
  - PostgreSQL 18、Redis、Celery：本次因 Windows Docker engine 未运行而未执行，不得写成已通过。
- 两个重构 GitHub Actions workflow 只监听 `codex/refactor-mainline`，而远端目前只有 `main`；截至审查时没有运行记录。

### 2.2 事实来源

| 来源 | 用途 | 状态 |
| --- | --- | --- |
| `AGENTS.md` | 项目协作、安全、临时测试和外部系统边界 | 强制遵守 |
| `docs/ai-context/short-term-context.md` | 当前业务约定和入口 | 开始前完整读取 |
| `docs/ai-context/long-term-knowledge-base.md` | 架构、业务链和长期约束 | 开始前完整读取 |
| `docs/ai-context/full-project-refactoring-execution-plan.md` | P0-P12 架构、阶段范围和退出门槛 | 架构与 DoD 事实源 |
| `docs/ai-context/goal-tasks.md` | 阶段状态事实源 | 必须更新 |
| `docs/ai-context/goal-log.md` | checkpoint 和验证证据 | 只追加紧凑当前状态，不重复历史流水 |
| `13c6d32e` | 当前 P7 checkpoint | 恢复基线 |

## 3. 范围与非范围

### 必须完成

- 修复并验证 P7 cancellation/readback/quota P1 缺陷。
- 修复 fixture UTF-8 读取。
- 更新当前 Goal 状态和压缩执行规则。
- 严格按 P7、P8、P9、P10、P11、P12 顺序完成 Local Code Complete。
- 每批保存 checkpoint commit，每阶段末只运行一次完整门禁。
- 清晰区分 `LOCAL_DONE` 与 `LIVE/HUMAN_PENDING`。

### 禁止执行

- 不修改、解密、反编译或导入 `web_app.py`/`pyarmor_runtime`。
- 不连接真实 Emby、115、MoviePilot、共享中心、Telegram、HDHive 或生产数据库。
- 不读取、挂载、复制或恢复旧 `/config`、旧数据库、Cookie、Session 或运行状态。
- 不部署、不发布 Release、不推送远端，除非用户在后续对话中明确授权。
- 不使用 `git reset --hard`、`git clean`、force push 或删除未知未跟踪文件。
- 不在同一 checkout 中运行两个并发 writer；Subagent 只能处理边界独立的只读审查或使用独立 worktree。
- 不为退出门槛之外的边缘情况新增通用框架、抽象层或依赖。
- 不把项目级验证写入临时 `tests/`；永久验证放在 `verification/`，临时 `tests/` 必须在提交前删除。

## 4. 必修缺陷

### TASK-001：修复 provider 写入与取消竞态

**涉及文件**

- `src/etk/domains/subscription/provider_execution.py`
- `src/etk/domains/subscription/operation_repository.py`，仅在状态约束确需加强时修改
- `verification/application/p7/test_subscription_provider_execution.py`
- `verification/integration/p7/test_subscription_runtime_postgres18.py`

**已确认根因**

`SubscriptionOperationExecutor._readback()` 在 provider write 已成功或可能已经发送后，如果取消请求同时到达且第一次 authoritative readback 尚未观察到目标状态，会调用 `mark_cancelled_before_write()`。该 terminal transition 会释放 quota、完成 Outbox 并停止恢复，和真实副作用状态不一致。

**修复约束**

- 只有能够证明 provider write 尚未调用时，才允许 `CANCELLED` 和 quota `RELEASED`。
- 一旦设置 `provider_request_started_at`，或 operation 已进入 `REMOTE_CONFIRMED`，取消不能根据一次旧 readback terminalize 为 `cancelled_before_provider_write`。
- 请求已启动后的未知/未观察状态继续进入 `AWAITING_READBACK`。
- 后续 readback 观察到目标状态时进入 `CONFIRMED`，quota `COMMITTED`。
- 达到恢复预算仍无法确认时进入 `INDETERMINATE`，quota 不得释放。
- 不重新发送已经 checkpoint 的 provider request。

**必须新增的回归场景**

1. provider create 成功，写入期间收到取消，第一次 readback 为空：结果必须是 `AWAITING_READBACK`，不能是 `CANCELLED`。
2. provider 请求 timeout/transport unknown，期间收到取消，第一次 readback 为空：继续 readback recovery。
3. 下一次 readback 观察到目标状态：结果 `CONFIRMED`，quota `COMMITTED`。
4. 达到最大 readback attempts 仍无法确认：结果 `INDETERMINATE`，quota 不得 `RELEASED`。
5. provider write 前收到取消：保留现有合法行为，结果 `CANCELLED` 且不调用 gateway。

**完成标准**

- 上述五个场景全部通过。
- PostgreSQL 18 集成测试证明 operation、quota 和 Outbox 在同一状态事实下收口。
- 不改变既有 HTTP envelope、MoviePilot 请求合同或幂等键。

### TASK-002：修复 UTF-8 fixture 读取

**涉及文件**

- `verification/application/p7/test_actor_subscription_http.py`

**修改**

把 UTF-8 JSON fixture 的 `.read_text()` 改为 `.read_text(encoding="utf-8")`。

**完成标准**

- 不依赖 `PYTHONUTF8=1` 即可收集并运行 P7 application tests。
- 搜索本次 P7 新增代码，确认没有其他新增的无编码文本读取。

### TASK-003：收口 P7 checkpoint 和压缩规则文档

**涉及文件**

- `docs/ai-context/full-project-refactoring-execution-plan.md`
- `docs/ai-context/goal-tasks.md`
- `docs/ai-context/goal-log.md`

**最小修改**

1. 把执行方案头部“尚未开始实现”改为“实施中；阶段状态以 `goal-tasks.md` 为准”。
2. 在效率规则中增加第 6 节所述压缩 cadence。
3. 在 P9 拆分 Local Fake/Stub gate 与 Live real-Emby gate。
4. 在 P10 明确后端合同冻结后只生成一次 TypeScript Client；合同变化才重新生成。
5. 在 P11 明确 ownership 100% 后执行一次受控 removal wave，但保留删除前清单和删除后 NFR 门禁。
6. P12 保留现有语义，只补自动化 harness、绝对路径/Compose Project/Volume guard 和证据包。
7. 在 `goal-log.md` 记录 `13c6d32e` 的实际实现范围、已验证证据、未运行门禁和下一批次。
8. 把 `goal-tasks.md` 的 P7 备注从“inventory in progress”改为实际 checkpoint 状态。

**紧凑日志格式**

```text
Stage / Batch:
HEAD / Commit:
Implemented:
Targeted gate:
Stage full gate:
Pending:
Blocker:
Next:
```
旧 evidence 保留，不重复复制，不把旧测试总数当作当前 HEAD 的结果。

## 5. P7-P12 执行卡

### TASK-004：闭合 P7

按以下三个批次执行，不重做 Actor/Watchlist/runtime：

1. **P7-FIX**：完成 TASK-001、TASK-002、TASK-003，运行针对性测试并提交 checkpoint。
2. **P7-HTTP/STATE**：一次完成 Resubscribe、Cleanup、核心 subscription routes、MoviePilot config、batch delete 及其 wiring/合同；对删除、额度、Outbox 和事务不变量运行 PostgreSQL 18 针对性测试。
3. **P7-RUNTIME**：一次完成 Subscribe Assistant 持久 Job/State 和剩余 native task handlers；验证重试、取消、超时、Worker 重启恢复及 Redis/Celery 进程路径。

P7 阶段末才统一：

- 更新全部 Manifest 和 OpenAPI；
- 运行 P7 + 全仓静态/测试/PG18/Redis-Celery 门禁；
- 对高风险事务做一次聚焦独立 review，再做一次阶段集成 review；
- 更新 Goal 状态并提交 P7 stage commit。

### TASK-005：完成 P8

只分两批：

1. **P8-LOCAL**：合集、封面、用户、发现、日志/统计等本地 CRUD、查询、权限和渲染逻辑。
2. **P8-EFFECTS**：系统更新、数据库维护和其他外部效果 Adapter；使用 Fake/Stub，验证权限、请求线程不执行长任务和失败映射。

封面必须使用固定 fixture 和图像差异阈值；用户权限和系统更新不得只做 happy-path CRUD 测试。

### TASK-006：完成 P9

只分三批：

1. **P9-LEDGER**：共享 Use Case、贡献点 Ledger、事务、幂等、并发扣减和补偿。
2. **P9-ADAPTERS**：共享中心、Telegram、HDHive 等 Adapter；只使用 Fake/Stub 和脱敏合同 fixture。
3. **P9-PROXY**：HTTP/WebSocket Proxy、虚拟 ID、分页、并发租约和资源回收；使用隔离测试服务完成合同与压力测试。

P9 本地门禁通过后标记 `LOCAL_DONE`。真实 Emby 起播、停止、WebSocket、并发限制和资源回收保持 `LIVE/HUMAN_PENDING`，不得伪造完成。

### TASK-007：完成 P10

1. P3-P9 后端合同冻结后生成一次 `frontend/src/api/generated/`。
2. 禁止手改 generated output；后端合同变化时重新打开合同阶段并重新生成。
3. 按页面族迁移 TypeScript、openapi-fetch 和 Vue Query，不逐文件单独跑全套验证。
4. 每个页面族结束运行一次 typecheck 和相关 Vitest。
5. P10 阶段末统一运行完整 typecheck、unit、build 和 Playwright E2E。
6. 保留现有 Router Path、Guard、响应 envelope、下载/上传/流式 Adapter 和产品视觉。

### TASK-008：完成 P11

P11 内部保持两个可定位检查点：

1. **Pre-removal**：Route/Task/Table/Config/Frontend ownership 100%，生成 legacy 删除清单、替代位置、依赖/import 扫描和镜像内容清单。
2. **Removal + NFR**：一次受控移除活动 legacy runtime，再独立运行安全、性能、可靠性、资源、SBOM、漏洞和 License 门禁。

可以归属于一个 P11 阶段，但不能把删除和验证做成无法定位的单一步骤。`web_app.py` 仓库文件本身的删除不包含在该动作中。

### TASK-009：完成 P12 Local Greenfield

使用一个只允许作用于 disposable local Greenfield 的自动化 harness 执行：

```text
create -> migrate -> verify -> capture evidence -> destroy
       -> rebuild from empty -> verify -> failure-rebuild drill
```

自动化必须：

- 要求显式、绝对、属于新环境允许根目录的目标路径；
- 要求隔离且可识别的 Compose Project 名称；
- 从 Compose labels 和解析配置核对将要操作的容器、网络和 Volume；
- 拒绝旧 `/config`、旧数据库路径、生产别名或未知 Volume；
- 销毁前保存镜像 Digest、PostgreSQL Server Version、Alembic revision、配置摘要、日志和测试报告；
- 初始化 PostgreSQL 18 空库并运行 `alembic upgrade head`；
- 验证 39 张 legacy compatibility 表和全部新 migration-owned 表，而不是使用过时总数；
- 默认关闭 Scheduler、Watchdog、Webhook 和真实外部写入；
- 只使用 Stub/Fake 和脱敏 fixture；
- 完成一次失败后清空新环境并从空状态重建的演练。

P12 本地完成不等于 Live Accepted。

## 6. 压缩执行规则

1. 每阶段只做一次事实盘点；没有新证据不重新扫描已确认范围。
2. 每阶段最多 2-3 个有完整输出的批次；一个批次结束才进入下一批。
3. 每批只运行一次受影响模块的 targeted verification。
4. 高风险批次必须立即验证事务、幂等、删除、额度、Ledger、Outbox、claim 和恢复不变量。
5. 完整 Ruff、Pyright、pytest、PG18、Redis/Celery、Manifest、OpenAPI、secret scan 只在阶段收口运行一次。
6. Manifest/OpenAPI 在阶段内可以暂时 stale，但必须记录为 pending；阶段 `DONE` 前必须重新生成并 `--check` 通过。
7. 每批 targeted gate 通过后创建 checkpoint commit；不要把数百个文件长期留在 dirty worktree。
8. 普通 CRUD 和机械前端迁移不做重复双 review；高风险事务做一次聚焦 review，阶段末做一次集成 review。
9. 不重新讨论已通过设计，除非测试失败或出现新的 P0/P1/P2 证据。
10. 不因为时间或 token 接近上限而降低完成门槛、伪造 `DONE` 或跳过状态记录。

## 7. 验证矩阵

### 7.1 每个 P7 批次的最小检查

```bash
uv run pytest -p no:cacheprovider verification/application/p7 verification/architecture/p7 -q
uv run ruff check <本批次涉及的 src/verification/migration 文件>
uv run ruff format --check <本批次涉及的 src/verification/migration 文件>
uv run pyright <本批次涉及的 src/verification 文件>
git diff --check
```

涉及事务、quota、Outbox、claim、删除或恢复的批次必须额外运行：

```bash
ETK_RUN_POSTGRES_INTEGRATION=1 \
uv run pytest -p no:cacheprovider verification/integration/p7 -q
```

默认 pytest 中的 opt-in skip 不能代替上述 PostgreSQL 18 结果。

### 7.2 P7 阶段末完整门禁

```bash
uv run python scripts/refactor/export_manifests.py
uv run python scripts/refactor/check_manifests.py
uv run python scripts/refactor/export_openapi.py
uv run python scripts/refactor/export_openapi.py --check

uv run ruff check src verification scripts
uv run ruff format --check src verification scripts
uv run pyright
uv run python -m compileall -q main.py src/etk migrations scripts/refactor verification
uv run pytest -p no:cacheprovider verification -q

ETK_RUN_POSTGRES_INTEGRATION=1 \
uv run pytest -p no:cacheprovider \
  verification/integration/p3 \
  verification/integration/p5 \
  verification/integration/p6 \
  verification/integration/p7 \
  verification/integration/platform -q

ETK_RUN_POSTGRES_INTEGRATION=1 \
ETK_RUN_CELERY_REDIS_INTEGRATION=1 \
uv run pytest -p no:cacheprovider \
  verification/integration/p4/test_celery_redis_runtime.py -q

uv run python scripts/refactor/scan_fixture_secrets.py verification/fixtures
uv run python scripts/refactor/scan_fixture_secrets.py verification/contracts/http
uv run python scripts/refactor/scan_fixture_secrets.py verification/contracts/db
git diff --check
```

P8-P9 按相同模式增加对应阶段目录；不重复运行无关的阶段中间门禁。

### 7.3 P10 前端阶段末门禁

```bash
npm --prefix frontend ci
npm --prefix frontend run typecheck
npm --prefix frontend run test:unit
npm --prefix frontend run build
npm --prefix frontend run test:e2e
```

### 7.4 P11-P12 额外门禁

- 依照原执行方案的 NFR 阈值输出性能和可靠性报告。
- 生成 SBOM、漏洞和 License 清单。
- 校验 `docker compose -f docker/compose.refactor.yml config`。
- P12 必须保存两次空状态创建结果和一次失败重建证据。
- `/api/health`、HTTP 200、容器 healthy 或页面打开不能单独作为完成证据。

## 8. Git、状态和证据规则

- 开始前运行 `git status --short --branch`、`git log -8 --oneline` 和 `git diff --check`。
- 只有 clean worktree 或完全解释清楚的已知 checkpoint 才能继续。
- 每批一个本地 checkpoint commit；不 amend 已有 commit。
- 阶段提交信息示例：
  - `fix(p7): preserve subscription readback after cancellation`
  - `refactor(p7): complete subscription http and state migration`
  - `refactor(p7): migrate subscription task runtime`
  - `refactor(p7): close subscription migration`
- 不 push、不部署、不发版；最终报告列出尚未推送的 commits。
- `goal-tasks.md` 是状态事实源；只有完整阶段门禁通过才能将阶段改为 `DONE`。
- 详细日志和测试输出保存在 artifact；Goal 主上下文只保留摘要和失败证据。

## 9. 停止条件

出现以下任一情况，停止当前高风险动作，保留 checkpoint 和证据，不得绕过：

- HEAD/工作树包含来源不明的修改，无法确认所有权。
- 发现真实外部地址、凭据或旧 `/config` 被新环境读取。
- 删除/销毁目标的绝对路径、Compose Project 或 Volume 无法证明属于 disposable Greenfield。
- quota、Ledger、删除、Outbox、claim 或状态机不变量失败。
- provider write 是否发送无法确认，却准备把 operation 标记为成功、失败或取消并释放额度。
- PostgreSQL 不是 18.x，Alembic head、表、索引或约束不一致。
- 出现未解决的 P0/P1 finding。
- 必需门禁无法执行时，保留为 pending；不能用默认 skip、health check 或 Fake 结果代替。

## 10. 最终报告格式

```text
Goal status:
Final HEAD:
Branch:

P7: DONE / DOING / BLOCKED
P8: DONE / TODO
P9 Local: DONE / TODO
P10: DONE / TODO
P11: DONE / TODO
P12 Local Greenfield: DONE / TODO
Live Acceptance: HUMAN_PENDING

Checkpoint commits:
- <commit> <scope>

Passed evidence:
- <command> -> <exact result>

Pending / not run:
- <gate> -> <reason>

Remaining P0/P1/P2 findings:
- <finding or none>

External actions:
- push: no
- deployment: no
- release: no
- real external writes: no
```

只有 P7-P12 Local 门禁全部通过且没有必需工作剩余时，才允许把 Goal 标记为 complete。Live Acceptance 始终单独报告。

## 11. 可直接粘贴给 Mac Codex 的 Goal 提示词

下面整段可直接作为 Goal objective 使用：

```text
继续完成 ETK 同仓库新明文主线的 P7-P12 全项目重构。当前远端审查基线是 origin/main@13c6d32ee546c103838130a0869a4b613f593b4f；P0-P6 已完成，P7 为 DOING，P8-P12 为 TODO，Live Acceptance 为 HUMAN_PENDING。

开始前完整读取并遵守：
1. AGENTS.md
2. docs/ai-context/short-term-context.md
3. docs/ai-context/long-term-knowledge-base.md
4. docs/ai-context/full-project-refactoring-execution-plan.md
5. docs/ai-context/goal-tasks.md
6. docs/ai-context/goal-log.md 的当前 P7 段；不要反复把整个历史日志注入上下文
7. 如果仓库中存在 docs/ai-context/p7-p12-mac-goal-repair-handoff.md，以它作为 P7-P12 执行 cadence 和已确认缺陷的控制增量

先执行只读 preflight：git status --short --branch、git log -8 --oneline、git diff --check，确认当前 HEAD 是 13c6d32e 或其可解释后继，且不存在来源不明的修改。禁止 git reset --hard、git clean、force push、删除未知未跟踪文件。不得在同一 checkout 中运行并发 writer。

第一优先级必须先修复 P7 cancellation/readback/quota P1 缺陷：provider write 已成功或可能已经发送后，如果取消同时到达且第一次 authoritative readback 尚未看到目标状态，当前代码会错误调用 mark_cancelled_before_write、释放 quota、完成 Outbox 并停止恢复。只有能证明 provider write 尚未调用时才允许 CANCELLED/RELEASED；请求 checkpoint 之后必须继续 AWAITING_READBACK，最终观察成功则 CONFIRMED/COMMITTED，恢复预算耗尽则 INDETERMINATE 且 quota 不得释放。补齐成功写入+取消+旧读、timeout unknown+取消+旧读、后续确认、最终 indeterminate、真正写入前取消五类测试，并运行 PostgreSQL 18 不变量测试。

同时把 verification/application/p7/test_actor_subscription_http.py 的 UTF-8 fixture 读取改为 read_text(encoding="utf-8")，确保测试不依赖 PYTHONUTF8 环境变量。

随后更新 Goal 文档：记录 13c6d32e 已完成的 subscription runtime、Actor/Watchlist routes、Cleanup/Resubscribe foundations、PostgreSQL operation state 和 process wiring；列出已验证及未运行门禁；把 Next 改成真实下一批。将执行方案状态改为实施中，并持久化以下压缩规则：每阶段一次盘点、2-3 个完整批次、每批一次 targeted gate、阶段末一次完整 Ruff/Pyright/pytest/PG18/Redis-Celery/Manifest/OpenAPI/secret scan、高风险事务一次聚焦 review、阶段末一次集成 review、每批 checkpoint commit、紧凑 Goal 日志。

P7 后续严格分批：
1. P7-FIX：上述 P1、UTF-8 和 Goal 文档修复。
2. P7-HTTP/STATE：完成 Resubscribe、Cleanup、核心 subscription routes、MoviePilot config、batch delete、wiring 和合同；删除、额度、Outbox、claim 必须运行 PostgreSQL 18 针对性门禁。
3. P7-RUNTIME：完成 Subscribe Assistant 持久 Job/State 和剩余 native task handlers；验证重试、取消、超时、Worker 重启恢复及 Redis/Celery 进程路径。
已通过的 Actor、Watchlist 和 operation runtime 不重新探索或重做，除非测试或 review 提供新的确定缺陷。

P7 阶段末才统一更新 Manifest/OpenAPI、运行完整门禁、完成高风险聚焦 review 和阶段集成 review，并在证据完整后把 P7 改为 DONE。

之后严格按顺序执行：
- P8 两批：本地 CRUD/查询/权限/封面渲染；外部效果、系统更新和数据库维护 Adapter。
- P9 三批：贡献点 Ledger；外部 Adapter；HTTP/WebSocket Proxy。只用 Fake/Stub。P9 本地通过标记 LOCAL_DONE；真实 Emby 起播、停止、WebSocket、并发和资源回收保持 LIVE/HUMAN_PENDING。
- P10：P3-P9 合同冻结后只生成一次 TypeScript Client；按页面族迁移，每族跑一次 typecheck+相关 Vitest，阶段末统一跑完整 typecheck、unit、build、Playwright。
- P11：先达到 Route/Task/Table/Config/Frontend ownership 100% 并生成删除清单，再一次受控移除 active legacy runtime，最后单独执行安全、性能、可靠性、资源、SBOM、漏洞和 License 门禁。不要删除仓库中的 web_app.py。
- P12：只在 disposable local Greenfield 上用安全自动化 harness 执行 create->migrate->verify->capture evidence->destroy->rebuild->verify->failure-rebuild。销毁前必须断言绝对路径、Compose Project、labels 和 Volume 全部属于新环境；禁止旧 /config、旧数据库、真实凭据和真实外部写入。

所有阶段保持 PostgreSQL 18、SQLAlchemy 2、psycopg 3、Alembic、FastAPI、Celery/Redis、Vue 3/TypeScript 既定架构；不新增微服务、Kubernetes、Kafka、全量 async 或退出门槛之外的抽象。不要修改、解密、反编译或导入 web_app.py/pyarmor_runtime。

每批只做必要的针对性测试，但高风险事务门禁不可省略；完整门禁只在阶段收口运行一次。默认 pytest 的 PostgreSQL/Redis/Celery skip 不能作为通过证据。HTTP 200、health、容器 healthy 或页面能打开不能单独作为完成证明。

每批 targeted gate 通过后创建独立本地 checkpoint commit，不 amend，不 push，不部署，不发布，不连接真实外部系统。goal-tasks.md 是状态事实源；只有阶段完整门禁通过才能标记 DONE。详细命令输出保存在 artifact，主上下文只保留 Stage/Batch、HEAD、Implemented、Targeted gate、Full gate、Pending、Blocker、Next。

遇到来源不明的工作树、真实外部访问、旧环境路径、销毁目标不明、数据库不变量失败、未解决 P0/P1、或者必需门禁无法执行时，停止对应高风险动作并保存证据，不得猜测、绕过或伪造完成。无法执行的 Live/外部门禁保持 HUMAN_PENDING。

持续执行直到 P7-P12 Local Code Complete 全部满足或出现不能自行解决的真实 blocker。最终报告必须列出最终 HEAD、每个 checkpoint commit、P7-P12 状态、每条实际通过的命令和结果、未运行项及原因、剩余 P0/P1/P2 finding，并明确 push/deploy/release/real external writes 均未执行。只有 P7-P12 Local 门禁全部完成且没有必需工作剩余时才把 Goal 标记为 complete；Live Acceptance 单独保持 HUMAN_PENDING。
```
