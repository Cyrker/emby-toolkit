# ETK 剩余重构执行计划：P11 收口与 P12 本地验收

> 核查日期：2026-09-08。交付方式：基于现有重构方案的精简开发交接，不新增架构或 SDLC 文档体系。
> 目标：持续执行到 P11、P12 Local Code Complete，或遇到有证据的真实阻塞；不在完成一个小修复后提前结束。
> 本文是执行合同，不是实时进度表。执行时保持本文只读，更新 `goal-tasks.md` 和紧凑的 `goal-log.md`。

## 状态更正（2026-09-12）

本计划的 TASK-001–007 已按日志完成，P11/P12 Local 当前均为 `DONE`，完整 LIVE 仍 `HUMAN_PENDING`。本文保留 2026-09-08 的执行合同；下文“剩余七个任务”、legacy 数量、dirty 草稿和 HEAD 都是当日起点，不是当前待办或回退目标。

日常接手读 [短期上下文](./short-term-context.md)、[长期知识库](./long-term-knowledge-base.md) 与 [goal-tasks.md](./goal-tasks.md)，按问题检索 [goal-log.md](./goal-log.md) 的 TASK 完成证据。共享资源中心等后续退役功能不因旧任务卡恢复；P12 销毁步骤不用于已部署的 Debian 环境。本次仅更新状态说明，不改历史验收标准。

## 1. 来源、范围与读取规则

| 来源 | 用途与效力 |
|---|---|
| 本计划及用户本次执行指令 | 剩余任务、顺序、减负规则；执行指令优先 |
| `full-project-refactoring-execution-plan.md` | 架构与验收基线；重点读取 §10 P11/P12、§11–13、§15 |
| `goal-tasks.md` | 阶段状态事实源；Notes 中的残余数量需要按代码校正 |
| 当前 Git、`verification/contracts/*ownership*`、实际 Worker 调用链 | 当前实现与归属证据；清单中的 `new` 不单独证明可执行 |
| `goal-log.md` | 历史验证与最新阻塞；按相关标题检索，不全文回放 |

首次启动读取适用的 `AGENTS.md`、短期上下文、长期知识库及上表指定范围；已由运行时完整提供的指令不重复读取。短期/长期背景中的旧 Flask 入口和旧环境命令不是新主线启动指南。

本文替代 `p11-p12-final-completion-handoff.md`、`p7-p12-mac-goal-repair-handoff.md` 中过时的 HEAD、dirty 清单、剩余任务和启动顺序；不降低原方案的业务合同、验收或安全边界。仅在需要追溯特定合同/命令时定位旧交接的对应段落。

必须实现：剩余原生任务、活动表/配置归属闭合、兼容运行时退出、P11 非功能门禁、P12 隔离重建演练。

不得实现：业务规则变更、接口重新设计、UI 重做、无关依赖升级、微服务/全量 async 化、新通用任务框架、生产迁移或真实外部写入。

## 2. 2026-09-08 已核实起点与矛盾（历史）

- 仓库：`/Users/cyrker/Desktop/project/emby-toolkit-refactor-mainline`。
- 分支：`codex/refactor-mainline`；HEAD：`d650f0e93b65bc99264c6c4b29eb6a7eebbb4a6c`。
- 最新 checkpoint：`refactor(p11): execute native resubscribe cache task`；`update-resubscribe-cache` 已有独立原生实现，禁止重做。
- P0–P8、P10 为 `DONE`；P9 为 `LOCAL_DONE`；P11 为 `DOING`；P12 为 `TODO`；Live 为 `HUMAN_PENDING`。
- 当前清单：HTTP `343/343` 路由、`367/367` 方法绑定为 `new`；前端 `24` 页面加 `1` 重定向全部为 `new`。
- 50 个 task key：`37 native-use-case + 4 typed-provider-adapter + 2 task-chain + 7 legacy-compatibility-callable`。两条 task-chain 也须验证包含未迁移成员时不能假成功。
- 表仍有 `12 legacy`；配置仍有 `82 dynamic + 7 bootstrap + 9 standalone legacy`。这些是清单残余，不等于全部需要重写。

已知矛盾与处理：

1. Tracker Notes/部分日志仍写剩余 8 个任务；当前实际剩余 7 个。启动核对后修正 Notes，P11 状态仍为 `DOING`，不能倒退已完成的缓存任务。
2. 日志曾记录订阅草稿不接线；当前 `worker.py` 已导入并构造 `NativeSubscribeAssistantMaintenanceGateway`，但目标 key 尚未加入 native-only allowlist。必须核对当前调用链，不能把“已构造”当作可执行验收。
3. 订阅草稿现有 3,126 行、封面草稿 511 行，均未跟踪；日志中的旧行数与历史通过数不代表当前工作树通过。
4. 原方案中的 `verification/e2e/api` 当前不存在；现有浏览器自动化入口在 `frontend/e2e/` 与 `frontend/package.json`。复用真实入口并补齐必要缺口，不能盲跑旧路径或把 shell smoke 当完整业务 E2E。

本次计划编制只做静态核查，`git diff --check` 通过；未重跑业务测试、PG18、镜像、NFR 或 Greenfield。

### 已知未提交工作

计划生成前有 10 个 tracked 修改：

```text
docs/ai-context/goal-log.md
docs/ai-context/goal-tasks.md
src/etk/domains/subscription/assistant.py
src/etk/domains/subscription/moviepilot.py
src/etk/domains/subscription/moviepilot_configured.py
src/etk/processes/p7_subscription_tasks.py
src/etk/processes/p8_effect_tasks.py
src/etk/processes/worker.py
verification/application/p4/test_process_roles.py
verification/application/p7/test_subscription_effects.py
```

已有 7 个 untracked：

```text
.DS_Store
src/etk/domains/subscription/task_execution.py
src/etk/domains/collection/cover_task_execution.py
verification/application/p7/test_subscription_task_execution.py
verification/application/p8/test_cover_task_execution.py
verification/integration/p7/test_subscription_task_execution_postgres18.py
verification/integration/p8/test_cover_task_execution_postgres18.py
```

本文是本次唯一新增的计划文件。末次检查另见若干子目录的 `.DS_Store`；这些文件均未操作，所有 `.DS_Store` 都不纳入提交，也不顺手删除。上述代码和测试保持原状接手；后续快照可能变化，应比较差异，不把此清单当永久锁定值。

## 3. 启动与执行纪律

启动只读运行一次：

```bash
git status --short --branch
git rev-parse --show-toplevel HEAD
git log -12 --oneline
git diff --stat
git diff --check
```

确认主线程为唯一 writer，结合 diff 解释已有修改。HEAD 或文件变化不自动构成阻塞；只有归属不明且与待改内容冲突、无法保留并继续时才询问用户。不得 reset、clean、stash、amend 或整体覆盖草稿。

执行者默认只有当前主线程；建议按 `core-goal-run` 与 `dev-spec-driven-implementation` 的适用规则执行，不自动启动 Subagent。沿用现有 P11/P12 tracker，不新增平行进度体系。

每个任务开始只列“缺什么 / 改哪里 / 如何验收”三项，然后实施。不反复重画架构、不机械拆文件；复用已有 Repository、Adapter、Unit of Work、claim/lease、Outbox、幂等与恢复机制。checkpoint 后禁止不确定外部写入重发；必要时只做权威查询确认并保留 `INDETERMINATE`。

## 4. 实施顺序与任务卡

来源统一为原方案 §10 P11/P12 与本计划 §2 的代码证据；允许范围为该任务及直接必要的调用链、配置、验证。禁止范围统一见 §1/§7。所有任务由主线程执行，不额外分配 agent。

### P11-A：5 个订阅任务，先收口当前草稿

**TASK-001 — 订阅助手巡检。**

- 目标/key：完成 `subscribe-assistant-maintenance`；范围为订阅 `assistant.py`、`task_execution.py`、MoviePilot adapter、P7 router 和 Worker 接线。
- 工作：保留有效修复，补齐既有巡检阶段、下载登记/进度/修复、历史对账及恢复；区分 provider 缺失、查询失败、权威空结果；外部操作不能凭对象 truthiness 判成功。
- VAL-001/完成：对应 application、PG18、Worker outcome/接线验证通过；重复投递、claim 失效、重启和不确定写入不导致漏处理、重发或假成功，原行为无缺口后才提升 ownership。
- 风险：高，存在外部操作、超时快照及持久化终态。

**TASK-002 — 智能追剧三任务。**

- 目标/key：完成 `add-all-series-to-watchlist`、`process-watchlist`、`refresh_completed_series`；范围为订阅领域及现有 Media/115/Sharing/Emby adapters。
- 工作：对照三个旧入口一次性列出行为差集；补足 MoviePilot、115、共享中心、元数据/图片和逐剧后续处理，复用新 owner，不能仅刷新 SQL/TMDb 状态就宣布完成。
- VAL-002/完成：增量/全量、空结果/失败、逐剧继续处理、取消、重复投递和恢复通过针对性验证；三个 key 均可 processor-free 执行，配置及用户可见行为不变。
- 风险：高，跨系统副作用与多步骤状态推进。

**TASK-003 — 媒体订阅删除。**

- 目标/key：完成 `resubscribe-library`；范围为既有洗版、Cleanup、MoviePilot/HDHive/115 adapters，不修改已完成的 `resubscribe_cache.py`，除非有直接失败证据。
- 工作：补齐旧行为中仍有效的本地文件/字幕/空目录/数据库清理、115 删除、Emby 刷新和 HDHive 分支；遇到与现行“不生成/读取 NFO”等合同冲突先确认，不能复活废弃能力。
- VAL-003/完成：范围/身份/开关校验、事务、取消、删除恢复及外部确认通过；当前草稿中的 unsupported 分支全部得到有效处理。业务允许的部分失败可保留，缺实现造成的 `partial` 不能作为迁移完成证据。
- 风险：最高，删除类操作；测试仅用 disposable 文件、数据库和 Fake/Stub。

顺序固定为 TASK-001 → TASK-002 → TASK-003。按任务族形成独立可验证 checkpoint，不把整份未完成草稿打包为成功提交。新文件必须自包含：确需抽出已完成部分时按实际职责最小拆分，不为整理目录重写其他草稿。

### P11-B：封面及归属闭合

**TASK-004 — 两个封面任务。**

- 目标/key：完成 `generate-all-covers`、`generate-custom-collection-covers`；范围为 `collection/cover_task_execution.py`、现有 renderer、P8 router/Worker。
- 工作：保留既有样式、配置、目标选择与上传行为；不更换渲染引擎，不新建封面系统。
- VAL-004/完成：两个 key 的渲染/上传、跳过/失败/取消、artifact 与重试恢复验证通过，实际 Worker 执行无 compatibility fallback。
- 风险：中高，文件产物与外部图片上传。

**TASK-005 — Table/Config 与活动归属收口。**

- 目标：按领域核对 §2 的残余，覆盖相关 repositories/settings/bootstrap、Worker/Coordinator/API 与 manifests。
- 工作：一次生成“旧读写者 → 当前新 owner → 缺口”；区分清单滞后与真实旧调用，仅替换缺口。保持 schema、配置键、默认值和优先级；不按表/配置逐项重写，不把所有 `Legacy*` 名字都当活动依赖。
- VAL-005/完成：原生启动、Worker/Coordinator 执行、import sentinels、配置/数据库合同和 manifest currentness 通过；Route/Task/Table/Config/Frontend 全部活动归属闭合，无未授权退休项、动态旧 import、旧 Processor 或不可执行任务。
- 风险：高，共享配置及持久化读写边界。只有实际必要的 schema 变化才提出最小方案，不擅自改变存量数据语义。

### P11-C：受控移除与阶段验收

**TASK-006 — 完成 P11。**

- 前置：TASK-001–005 全部通过，且无未解决 P0/P1/P2。
- 工作：先保存 pre-removal 清单，逐项记录旧活动入口、新替代位置、引用/import 扫描、镜像内容与验证证据，并建立本地 checkpoint；然后一次受控移除活动 WSGI fallback、旧 Processor/TaskManager/SchedulerManager、启动建表和旧依赖。仅退出活动运行路径；不批量删除历史源码。
- VAL-006/完成：运行 §5 完整阶段门禁及原方案全部 P11 NFR/security 要求；镜像不包含 `web_app.py`/PyArmor runtime，原生进程不依赖旧运行时；关键合同全部通过，无 P0/P1/P2，才将 P11 改为 `DONE`。
- 风险：高，整套运行环境切换。失败时不进入 P12，保留 checkpoint 和诊断证据；回退不得覆盖已有未提交工作，不使用破坏性 Git 命令。

### P12：仅隔离本地 Greenfield

**TASK-007 — 空状态创建、重建和失败演练。**

- 前置：P11 `DONE`；范围为现有 `Dockerfile.refactor`、`docker/compose.refactor.yml`、迁移与验证工具，缺少编排时只补最小 harness。
- 工作：`create → migrate → verify → capture → destroy → rebuild from empty → verify → failure-rebuild drill`。使用 PostgreSQL 18、Redis、锁定镜像、全新运行状态和 Fake/Stub；覆盖原方案要求的数据库不变量、任务恢复、关键业务 E2E 与模拟 canary。
- 隔离：销毁前核对 allow-root 下的绝对 realpath、独立 Compose Project、resolved mounts、容器/网络/volume labels。容器内新 `/config` 可用，但不能复用旧 `/config` 数据；未知 volume、生产别名、其他项目资源一律拒绝操作。端口仅限本机测试，控制面不得操作其他项目；Docker socket 的 `:ro` 不能单独作为 API 权限隔离证据。
- VAL-007/完成：保存首次空状态验证、第二次重建验证、一次失败后清空重建三组证据，包含 image digest、PG version、Alembic revision、resolved Compose、资源归属、脱敏配置摘要、日志和测试报告；P12 才可 `DONE`，Live 仍为 `HUMAN_PENDING`。
- 风险：高，销毁只限归属已证明的 disposable 资源；单独 healthy、HTTP 200、页面打开不能代替闭环验收。

## 5. 分层验证：不重复全套，也不降低标准

| 时机 | 必需验证 |
|---|---|
| 开发中 | 最小复现及对应回归；仅改动文件的 Ruff/类型检查 |
| 每个可交付任务族 | 一次正式 targeted gate：相关 application + 真实 PG18 + Worker 分派/outcome/恢复；涉及队列时验证 Redis/Celery；高风险事务做一次聚焦 review |
| 归属提升时 | 对应 exporter/overlay 更新及 `check_manifests.py`；不能仅改 manifest 掩盖 owner 缺失 |
| P11 阶段末 | 一次完整 Ruff/format/Pyright、全部适用 verification、PG18、Redis/Celery、前端 type/unit/build/API-boundary/E2E、OpenAPI/manifest、secret scan、lock/compile、Compose/image/import、docs build、覆盖率及 NFR/security/SBOM/漏洞/License；一次集成 review |
| P12 | 上一节三组独立环境证据；环境重建改变了状态，必须重新验证，不能复用前一环境的通过结果 |

“一次”指当前版本正式门禁不无故重复；修复后必须重跑受影响门禁，跨域改动时扩展范围。没有新失败、改动或风险证据，不重审 P7–P10，不重新跑已经有效的无关验证。

现有命令入口示例，不是 TASK 完成的全部标准：

```bash
uv run pytest -q -p no:cacheprovider verification/application/p7/test_subscription_task_execution.py
uv run pytest -q -p no:cacheprovider verification/application/p4/test_worker_outcomes.py
uv run pytest -q -p no:cacheprovider verification/application/p8/test_cover_task_execution.py
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest -q -p no:cacheprovider verification/integration/p7/test_subscription_task_execution_postgres18.py
ETK_RUN_POSTGRES_INTEGRATION=1 uv run pytest -q -p no:cacheprovider verification/integration/p8/test_cover_task_execution_postgres18.py
uv run python scripts/refactor/check_manifests.py
```

PG 命令前确认 fixture/连接仅指向 disposable PG18，断言真实服务器版本；skip 不算通过。既有草稿测试只覆盖部分行为，需补足任务卡要求，而不是只跑现有测试就提升归属。Python 沿用项目的 3.12/uv，不使用本机系统 Python 代替运行时。

仅变更 HTTP/OpenAPI/前端合同或进入阶段末时，运行相应 API 生成与前端门禁；无需每个后端小修复都重新生成客户端。`npm --prefix frontend run test:e2e` 是已有自动化入口，运行前确认配置及隔离目标。

P11 NFR 保留原门槛：健康 p95 ≤100ms；纯 DB API 不高于旧基线 110%、目标 ≤250ms；任务创建 p95 ≤500ms；API 无超过 500ms 的 CPU/阻塞工作；外部 client 有 timeout/有限重试/并发上限；Worker 强制终止后无永久 RUNNING；日志标识完整；关键代码分支覆盖率 90%、改动行覆盖率至少 85%；Bridge/贡献点/删除/幂等/认证合同全部通过。缺基线、工具或证据要明确记录，不能自降标准、伪造比较结果或执行受保护旧入口。

## 6. 进度、证据与停止条件

- 完成一个可独立理解的任务族，运行对应 targeted gate 并创建独立本地 checkpoint；只提交该任务已验收的文件/hunk，不夹带 `.DS_Store`、秘密、临时产物或其他未完成草稿。不 amend、不 push。
- `goal-tasks.md` 保留 P11/P12 总状态；子任务进度以 TASK-001–007 在 `goal-log.md` 中紧凑记录，不创建第三套 tracker。
- 每次日志仅记 `TASK / HEAD / Implemented / Gate / Pending / Blocker / Next`，详细命令、stdout 和报告放 `/tmp/etk-refactor-artifacts/` 下对应批次目录；不能把秘密写入日志或公开产物。
- 不因一个测试通过、一个 checkpoint 完成或普通可修复失败而结束。持续推进下一个已定义任务，直到 P11/P12 Local 完成或真实阻塞。
- 同一问题没有新证据时不循环重读/重审；定位最小复现并报告现象、根因证据、已尝试修复及下一步。需改变业务/兼容策略、无法证明删除归属、必须门禁受外部条件阻挡或存在冲突 writer 时停止受影响写入并询问用户；不擅自跳过必需任务标完成。
- 最终报告：HEAD、新 checkpoint、7 个 key 最终 owner、表/配置归属、实际门禁及未运行项、P11 NFR/security 结果、P12 三组证据、剩余 P0/P1/P2、最终阶段状态和下一步。

## 7. 全程不可越界

只允许本地源码/验证/进度记录、本地 checkpoint、隔离 PG18/Redis/Celery 和归属已证明的 disposable Greenfield。真实 Emby/115/TMDb/MoviePilot/Telegram/HDHive/共享中心/生产数据库及旧运行数据不参与本地验收；使用 Fake/Stub/loopback/脱敏 fixture。

禁止 push、部署、Release、force 操作、`--prune`、未知文件删除、生产连接变更和真实外部写入。禁止修改、导入、执行、删除、解密或反编译 `web_app.py`/PyArmor runtime。`verification/` 是项目验证目录；不得将临时 `tests/` 纳入提交，也不能以清理临时文件为由删除来源不明的既有内容。

最终只交付 Local Code Complete；Live Acceptance 始终独立保留 `HUMAN_PENDING`，不得把本计划解释为生产启用或外部写入授权。
