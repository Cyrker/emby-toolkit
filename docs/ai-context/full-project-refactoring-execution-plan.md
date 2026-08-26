# ETK 全项目重构执行方案

> 文档编号：`ETK-RF-001`
> 状态：实施中；阶段状态以 `goal-tasks.md` 为准
> 编制日期：2026-08-22
> 代码基线：`main@00786dcd`，应用版本 `10.8.65`
> 适用范围：当前仓库中的旧版 Emby ToolKit，不代表上游 ETKvNext 仓库的实现计划

## 1. 决策摘要

本项目采用以下已经确认的重构策略：

> **同一仓库、新明文主线、按域全部迁移、全新环境一次启用。**

这意味着：

- 最终目标是替换当前项目的全部活动运行路径，而不是只拆几个大文件。
- 不修改、不解密 `web_app.py` 的 PyArmor 载荷；新主线重新实现明文启动装配。
- 迁移期间保留现有 API、数据库结构与业务语义、配置、任务 Key、前端路由和外部协议；历史数据库行数据不迁移。
- 旧代码只作为行为参考和迁移期兼容实现；当前运行环境不是数据源、部署目标或回退目标。
- 新主线只部署到全新 Compose 环境；新环境通过全部验收后一次启用外部访问、Webhook、调度和后台写入。
- 目标架构是模块化单体，不拆微服务，不引入 Kubernetes，不进行无收益的“全量 async 化”。
- 新主线数据库服务器固定为 PostgreSQL 18；使用全新数据目录和空库初始化，不迁移当前环境中的任何状态。

### 1.1 最终完成标准

满足以下全部条件，才算“全项目重构完成”：

1. 生产启动链不再导入或执行 `web_app.py`、`pyarmor_runtime_000000`。
2. 当前所有外部可见 API 均由新主线提供，兼容性清单没有未解释差异。
3. 当前 50 个任务 Key 均有明确的新实现、替代关系或经批准的删除记录。
4. 当前 39 张业务表均有明确所有者、SQLAlchemy 映射和 Alembic 基线。
5. 现有 Vue 页面和路由均迁移到 TypeScript 与统一 API Client。
6. 后台任务、调度器、文件监控和常驻 daemon 不再隐式启动于 API 进程。
7. Flask、gevent、psycopg2、`requests` 和全局 `extensions` 不再属于活动主线依赖。
8. 关键业务契约、数据库不变量、外部适配器和端到端流程均有可重复验证。
9. 完成至少一次全新环境销毁重建演练和一次启用失败后的停止/重建演练。
10. 新环境的稳定观察期结束后，确认新部署可独立维护；旧环境清理由单独任务处理。

### 1.2 PostgreSQL 18 空库初始化决策

当前环境已明确没有保留价值，因此本方案不做 PostgreSQL 16 物理升级、`pg_upgrade`、历史业务数据迁移或旧 Volume 回退。目标方式固定为：

```text
新 PostgreSQL 18 容器
  └─ 新数据目录 / 新 Volume
       └─ alembic upgrade head
            └─ 初始化空的目标 Schema
```

边界如下：

- 保留当前 39 张表的业务语义作为第一版兼容 Schema，但不保留现有表内记录。
- 不把 PostgreSQL 16 的物理数据目录挂载给 PostgreSQL 18。
- 不从旧生产库导入媒体、订阅、追剧、整理、任务、用户、共享或贡献点历史记录。
- 新环境使用全新的 `/config`，不复制旧配置、Token、图片、会话文件或其他运行状态；授权通过安全来源重新配置。
- 通过 Emby/115 可以重建的数据由明确的初始化任务重建；不能重建的数据视为已批准放弃。
- 旧数据库、旧 `/config`、旧镜像和旧容器不属于新环境验收或回退链路。
- P12 只确认新环境的目标主机、Compose Project、端口、挂载、Volume 和外部写入范围；不再确认旧容器细节。
- 停止或删除当前环境不是本重构方案的一部分。若当前实例仍连接相同 Emby、115、Webhook 或调度目标，启用新环境前只需确认它已停止消费；实际删除另行点名执行。

## 2. 目标与非目标

### 2.1 目标

- 建立完全明文、可测试、可组合、可观测的应用启动与生命周期。
- 将路由、业务规则、数据库、外部 HTTP 和后台副作用分离。
- 把巨型 Processor/Route/Handler 拆成按领域组织的应用服务和适配器。
- 为任务提供持久状态、重试、幂等、取消、心跳和崩溃恢复能力。
- 让 API、Worker、Scheduler、Monitor 和 Proxy 具有明确进程边界。
- 用 OpenAPI 和类型生成减少前后端接口重复定义。
- 让数据库结构由 Alembic 管理，不再依赖启动时散落的建表和迁移 SQL。
- 在不改变业务语义的前提下，提高吞吐、故障隔离能力和开发效率。

### 2.2 本轮重构明确不做

- 不破解、反编译或修改 PyArmor 载荷。
- 不在重构阶段重新设计产品功能、收费规则、贡献点规则或权限语义。
- 不把 PostgreSQL 换成其他数据库。
- 不把 Vue 换成 React，也不更换现有 Naive UI 视觉体系。
- 不为了框架迁移改变现有 URL、JSON envelope、HTTP 状态码或 Cookie 语义。
- 不在缺少业务基线时合并、重命名或删除现有表和字段。
- 启用新环境前，不允许任何旧实例继续处理相同媒体、Webhook、任务或共享中心事件。
- 不把单体拆成远程微服务；进程角色可以分离，但代码和发布单元保持一个仓库、一个版本。

## 3. 当前状态基线

### 3.1 已核实规模

| 项目 | 当前值 | 核实方式 |
| --- | ---: | --- |
| Python 文件 | 126 | 仓库文件清单，排除依赖目录 |
| Vue 文件 | 55 | `frontend/src/**/*.vue` |
| Flask Blueprint | 22 | `routes/*.py` 中 `Blueprint(...)` |
| 路由装饰器 | 343 | `routes/` 与 `reverse_proxy.py` 静态扫描 |
| 注册任务 Key | 50 | `tasks/core.py:get_task_registry()` AST 解析 |
| 初始化 PostgreSQL 表 | 39 | `database/connection.py` 建表 SQL 解析 |
| 项目级测试路径 | 0 | 排除依赖目录后的 `test/tests/spec/specs` 文件扫描 |

仓库中只有两个 Handler 文件内嵌了 `unittest`：

- `handler/p115_media_recognition.py`
- `handler/tg_media_candidate_flow.py`

它们不足以构成重构所需的系统行为基线。

### 3.2 主要技术债证据

| 文件 | 当前行数 | 主要问题 |
| --- | ---: | --- |
| `handler/p115_service.py` | 11544 | 115 Client、整理、缓存和业务编排集中 |
| `tasks/shared_resource_tasks.py` | 8680 | 共享中心批任务和业务规则高度耦合 |
| `handler/shared_subscription_service.py` | 5393 | 共享订阅状态机和外部调用混合 |
| `core_processor.py` | 4839 | 元数据聚合、翻译、持久化和 Emby 回写集中 |
| `tasks/p115.py` | 3500 | 长任务、daemon、状态与网盘副作用混合 |
| `tasks/media.py` | 3377 | 媒体任务和核心处理器边界不清 |
| `routes/p115.py` | 3353 | HTTP 参数、业务规则和外部调用混合 |
| `handler/intro_detection_service.py` | 3211 | 队列、指纹计算、持久化和 Emby 回写混合 |
| `routes/shared_resource.py` | 3032 | 路由、共享业务与中心调用混合 |
| `routes/webhook.py` | 2821 | 多种外部事件共用巨型入口 |
| `watchlist_processor.py` | 2688 | 追剧状态、订阅和媒体处理耦合 |
| `reverse_proxy.py` | 2165 | HTTP/WebSocket 代理、虚拟库和播放状态混合 |

其他已确认问题：

- `extensions.py` 保存处理器、服务端 ID、任务注册表和多个进程内可变集合。
- `database/connection.py` 在应用启动时创建和修改表，迁移历史缺少版本化账本。
- 路由、任务和 Service 之外仍存在大量直接数据库事务和外部 HTTP 调用。
- 依赖文件大部分未锁定版本，构建不可完全复现。
- 前端约有 412 处 API/fetch 使用模式，缺少统一、类型安全的 API 契约层。
- API、Worker、Scheduler、Watchdog、115 daemon 和代理的生命周期由旧入口集中装配。

### 3.3 PyArmor 边界

当前 `web_app.py` 是三行 PyArmor Loader。静态搜索确认，直接从它导入业务函数的只有：

- `routes/system.py:565`：导入 `save_config_and_reload`
- `routes/unified_auth.py:86`：导入 `save_config_and_reload`

其他模块主要通过 `extensions.py` 获得运行时对象。因此新主线可以：

1. 重建明文应用工厂和运行时装配。
2. 将上述两处调用迁移到新的 `RuntimeSupervisor`。
3. 通过兼容注册表向尚未迁移的模块提供旧式对象。
4. 在迁移完成后移除兼容注册表。

历史明文提交 `f94afbc4426f9997780ff838a6aa3d032ba9474d` 只用于理解启动职责，不作为当前 PyArmor 载荷逐字等价的证据。任何启动行为都必须通过当前隔离运行环境重新验证。

### 3.4 当前启动拓扑

```text
docker/entrypoint.sh
  ├─ python3 /app/web_app.py generate-nginx-config
  ├─ nginx -g "daemon off;"
  └─ python3 /app/web_app.py
       ├─ Flask + 22 Blueprints
       ├─ gevent WSGIServer :5257
       ├─ reverse_proxy proxy_app
       ├─ TaskManager worker thread
       ├─ APScheduler
       ├─ Watchdog MonitorService
       ├─ 115 daemon
       └─ optional shared/TG services
```

该拓扑的核心问题是：导入和启动发生副作用，API 多 Worker 扩容会重复启动后台服务，任意一个常驻组件异常也可能影响整个 Web 进程。

## 4. 白话边界：保留、替换、删除

### 4.1 第一阶段必须保留

- 所有现有 URL、HTTP Method、参数名称、默认值和响应 envelope。
- 现有 Cookie 名称、签名密钥来源、Session Key、管理员判定和登录状态响应。
- PostgreSQL 现有 39 张表、字段、索引、主键和业务语义；历史行数据不迁移。
- `config.ini`、`app_settings` 和现有配置键的读取优先级及保存格式。
- 50 个任务 Key、任务链顺序、手动/调度可见性、进度和停止语义。
- Vue 页面路径、权限守卫、主要交互和 Naive UI 视觉体系。
- Emby、115、TMDb、豆瓣、MoviePilot、Telegram、共享中心、影巢和 Bridge 的外部契约。
- `/config`、媒体目录和图片仓库的容器内路径语义；新环境使用全新目录内容。
- 根目录 `LICENSE` 的 AGPLv3 要求和现有版权声明。

### 4.2 按阶段替换

- `web_app.py` 的启动装配 → `main.py` + `src/etk/bootstrap/`。
- Flask 主应用 → FastAPI 主应用；未迁移 Flask Blueprint 暂由 WSGI 兼容层挂载。
- `extensions.py` 全局对象 → 显式 `AppContainer`、构造函数参数和端口接口。
- `psycopg2` 函数式数据库模块 → SQLAlchemy 2 Repository + Unit of Work。
- 启动时建表 SQL → Alembic 版本化迁移。
- `requests` 与散落重试 → 按外部系统拆分的 `httpx` Client Adapter。
- 进程内 TaskManager → Celery + Redis Broker + PostgreSQL 任务运行记录。
- API 内 APScheduler → 独立 Coordinator 进程，仅负责向队列投递任务。
- API 内 Watchdog/daemon → 独立 Coordinator/Agent 进程。
- 巨型 Processor/Handler → 领域服务、应用用例、Repository 和 Integration Adapter。
- 前端散落 Axios → OpenAPI 生成的 TypeScript Client + Vue Query。

### 4.3 最终从活动主线删除

- `from web_app import save_config_and_reload`。
- 新代码对 `extensions.media_processor_instance` 等全局变量的依赖。
- API 进程导入即启动线程、daemon、scheduler 或连接外部系统的副作用。
- Flask/gevent 兼容挂载和旧 Blueprint 注册。
- `psycopg2`、`requests`、旧 TaskManager 和旧启动依赖。
- 已完成迁移且通过合同验收的旧 Processor、Route、Task、Handler 实现。
- Docker 镜像中的 `web_app.py` 和 `pyarmor_runtime_000000`。

`web_app.py` 文件本身不修改，在迁移期只作为源码行为参考。全新环境的镜像从一开始就不把它作为启动入口，P11 后完全排除 PyArmor runtime；删除仓库文件属于单独决策。

## 5. 目标架构

### 5.1 架构类型

采用 **模块化单体 + Ports and Adapters + 分进程运行角色**：

- 一个仓库。
- 一个应用版本。
- 一个业务数据库。
- API、Worker、Coordinator、Proxy 使用同一套领域和应用代码。
- 进程之间通过 Redis 任务消息和 PostgreSQL 持久状态协作。
- 模块之间通过应用接口或领域事件协作，不直接读取对方私有 Repository。

这比微服务更适合当前规模：它消除巨型模块和全局状态，但不引入跨服务版本、远程事务、服务发现和分布式部署成本。

### 5.2 目标运行拓扑

```text
                         ┌──────────────────────────────┐
Browser / Emby / Bridge ─► Nginx                        │
                         │  /assets -> static           │
                         │  /api    -> API :5257        │
                         │  proxy   -> Proxy :8097      │
                         └──────────────┬───────────────┘
                                        │
                   ┌────────────────────┼────────────────────┐
                   ▼                    ▼                    ▼
             FastAPI API          ASGI Proxy          Coordinator
             - auth/session       - virtual lib       - scheduler
             - OpenAPI            - playback          - watchdog
             - short request      - websocket         - singleton daemons
                   │                    │                    │
                   └──────────────┬─────┴────────────────────┘
                                  ▼
                         Application / Domain
                                  │
             ┌────────────────────┼────────────────────┐
             ▼                    ▼                    ▼
       PostgreSQL 18          Redis 7              Celery Worker
       source of truth        broker/cache         long jobs
             │                                         │
             └────────────────────┬────────────────────┘
                                  ▼
                 Emby / 115 / TMDb / MoviePilot /
                 Shared Center / Telegram / HDHive
```

### 5.3 目标目录

```text
emby-toolkit/
├─ main.py                         # 唯一明文 CLI/进程入口
├─ pyproject.toml                  # Python 依赖与工具配置
├─ uv.lock                         # 可复现依赖锁
├─ src/
│  └─ etk/
│     ├─ bootstrap/
│     │  ├─ app_factory.py         # FastAPI 工厂，无启动副作用
│     │  ├─ container.py           # 显式依赖装配
│     │  ├─ lifecycle.py           # 进程级启动/停止
│     │  ├─ settings.py            # Pydantic Settings + 旧配置适配
│     │  └─ cli.py                 # api/worker/coordinator/proxy/nginx-config
│     ├─ compatibility/
│     │  ├─ flask_app.py           # 迁移期 Flask WSGI fallback
│     │  ├─ legacy_registry.py     # 迁移期 extensions 兼容写入
│     │  ├─ session_codec.py       # Flask Cookie/Session 兼容
│     │  └─ response_contract.py   # 旧响应与错误映射
│     ├─ platform/
│     │  ├─ db/                    # Engine、UnitOfWork、Alembic
│     │  ├─ jobs/                  # Celery、任务状态、幂等与取消
│     │  ├─ events/                # Domain Event / Outbox
│     │  ├─ http/                  # 公共 HTTP policy
│     │  ├─ observability/         # 日志、指标、Trace ID
│     │  └─ security/              # Session、权限、敏感值过滤
│     ├─ modules/
│     │  ├─ system/
│     │  ├─ identity/
│     │  ├─ media/
│     │  ├─ ingestion/
│     │  ├─ p115/
│     │  ├─ subscription/
│     │  ├─ people/
│     │  ├─ collection/
│     │  ├─ sharing/
│     │  ├─ users/
│     │  └─ operations/
│     ├─ integrations/
│     │  ├─ emby/
│     │  ├─ bridge/
│     │  ├─ tmdb/
│     │  ├─ douban/
│     │  ├─ p115/
│     │  ├─ moviepilot/
│     │  ├─ shared_center/
│     │  ├─ telegram/
│     │  └─ hdhive/
│     └─ processes/
│        ├─ worker.py
│        ├─ coordinator.py
│        └─ proxy.py
├─ frontend/
│  └─ src/
│     ├─ api/generated/            # OpenAPI 生成，禁止手改
│     ├─ api/queries/              # Vue Query hooks
│     ├─ modules/                  # 与后端领域对应
│     ├─ stores/                   # 仅客户端/auth/UI 状态
│     └─ router/
├─ verification/                   # 永久验证资产，不使用 tests/ 目录
│  ├─ unit/
│  ├─ integration/
│  ├─ contracts/
│  ├─ e2e/
│  ├─ performance/
│  └─ fixtures/
├─ scripts/refactor/               # 契约导出、差异比较、演练脚本
├─ migrations/                     # Alembic revisions
└─ docker/
   ├─ entrypoint.sh
   ├─ nginx/
   └─ compose.refactor.yml
```

### 5.4 每个领域模块的固定内部结构

```text
modules/<domain>/
├─ api/              # FastAPI Router、HTTP DTO、兼容映射
├─ application/      # Use Case、Command、Query、事务编排
├─ domain/           # Entity、Value Object、Policy、Domain Event
└─ infrastructure/   # SQLAlchemy Repository、外部 Adapter 实现
```

依赖规则：

1. `domain` 不导入 FastAPI、SQLAlchemy、Celery、Redis 或具体外部 Client。
2. `application` 只依赖领域对象和抽象 Port。
3. `api` 负责协议转换，不直接执行 SQL 或外部 HTTP。
4. `infrastructure` 实现 Repository/Port，但不反向调用 API。
5. 跨领域写操作必须调用对方 Application Service 或投递领域事件。
6. 禁止新建通用 `utils.py`、`helpers.py` 继续堆积跨域逻辑。

## 6. 明确技术栈

“先进”在本方案中指稳定、类型安全、可观测、可测试和可演进，不等于盲目采用最新或最复杂的组件。

| 层级 | 目标技术 | 决策 |
| --- | --- | --- |
| Python Runtime | Python 3.12 | 本次重构不夹带解释器升级；兼容当前 p115/Telethon/ffmpeg 生态。3.13+ 单独评估 |
| 包与构建 | `uv` + `pyproject.toml` + `uv.lock` | 替换未锁定的 `requirements.txt`；Docker 仍可导出 requirements 兼容文件 |
| API | FastAPI + Starlette + Uvicorn | OpenAPI、类型 DTO、ASGI、生命周期清晰 |
| 数据校验 | Pydantic v2 + pydantic-settings | API DTO、配置和边界校验；数据库 Entity 不直接当 API DTO |
| 数据库服务器 | PostgreSQL 18 | 新建数据目录并由 Alembic 初始化空库；不执行 16→18 物理升级或历史数据导入 |
| 数据访问 | SQLAlchemy 2.x + psycopg 3.x + psycopg_pool | 统一同步 Repository；FastAPI 的同步 Use Case 进入线程池，避免全量 async 污染 |
| 迁移 | Alembic | 首先建立当前 39 表无差异基线，再只做向前兼容迁移 |
| 任务 | Celery 5.x + Redis 7.x | 长任务脱离 API；Redis 是 Broker，不是业务事实来源 |
| 调度 | 独立 Coordinator + APScheduler 3.x | 只计算计划并向 Celery 投递，不直接执行任务；4.x 升级另做兼容验证 |
| 外部 HTTP | httpx + tenacity | 每个外部系统独立 Client、timeout/retry/rate-limit/idempotency policy |
| 领域事件 | 进程内事件 + PostgreSQL Transactional Outbox | 需要可靠异步副作用时落 Outbox；不引入 Kafka |
| 缓存/锁 | Redis + PostgreSQL advisory lock | Redis 用短期缓存；涉及数据正确性的单例锁使用 PostgreSQL |
| 前端 | Vue 3.5 + TypeScript 5.x + Vite | 保留现有框架与 UI，迁移到严格类型 |
| 前端状态 | Pinia 3 + TanStack Vue Query | Pinia 管 auth/UI；服务端状态交给 Query Cache |
| API Client | OpenAPI TypeScript + openapi-fetch | 从后端契约生成，停止手写 URL 和重复 DTO |
| UI | Naive UI 2.x；保留必要 Ant Design Vue 兼容 | 不在架构迁移中重做视觉；逐页消除双 UI 库时必须有单独验收 |
| 后端验证 | pytest + pytest-cov + Testcontainers | 放在 `verification/`，不违反项目 `tests/` 临时目录约定 |
| 前端验证 | Vitest + Vue Test Utils | 组件、Store 和 Query Hook 验证 |
| E2E | Playwright，仅用于自动化验收 | 本地人工调试仍优先使用 Codex Browser/CDP |
| 代码质量 | Ruff + Pyright | Ruff 负责格式/静态规则，Pyright 分阶段提高严格度 |
| 日志 | stdlib logging + structlog JSON renderer | 保留可读控制台，生产提供结构化字段和 `trace_id/job_id` |
| 指标 | prometheus-client | API、任务、外部调用、队列、DB Pool 和常驻服务指标 |
| 部署 | Docker BuildKit + Docker Compose | 同一镜像按不同 command 运行 API/Worker/Coordinator/Proxy |
| 入口代理 | 独立 Nginx 容器 | 静态资源、5257 API 和 8096/8097 代理边界明确 |

### 6.1 版本锁定规则

- 上表锁定架构和主版本方向，具体 minor/patch 在阶段 P1 建立兼容矩阵后写入 `uv.lock` 与 `package-lock.json`。
- 禁止在领域迁移 PR 中顺带升级无关依赖。
- 所有升级必须先通过 Linux 容器、Windows 本地和目标 CPU 架构构建验证。
- Python 3.13+、APScheduler 4、Vue/Vite 新 major 均不属于本次重构完成条件。

### 6.2 为什么不全量 async

当前大量逻辑使用同步 p115 Client、ffmpeg 子进程、文件系统操作和同步第三方 SDK。机械改成 `async def` 会制造阻塞 Event Loop、双套 Repository 和更难调试的取消问题。

本方案采用：

- HTTP/API 层使用 ASGI。
- 短同步 Use Case 由 FastAPI 线程池执行。
- 长任务进入 Celery Worker。
- WebSocket、流式代理和明确的并发外部请求使用 async。
- 只有性能数据证明有收益的 Repository/Client 才单独引入 Async 实现。

## 7. 领域边界与数据所有权

| 目标领域 | 当前主要来源 | 第一阶段拥有的数据 |
| --- | --- | --- |
| `system` | `config_manager.py`、`routes/system.py`、`scheduler_manager.py` | `app_settings`、运行配置、调度配置 |
| `identity` | `routes/unified_auth.py`、`extensions.py`、`database/user_db.py` | `emby_users`、`emby_users_extended`、`invitations`、Session 契约 |
| `media` | `core_processor.py`、`routes/media.py`、metadata DB | `media_metadata`、图片缓存/策略、人物元数据 |
| `ingestion` | `monitor_service.py`、Bridge 回调、媒体入库任务 | 入库编排、ItemID 绑定、媒体身份事件 |
| `p115` | `routes/p115.py`、`handler/p115_*`、`tasks/p115.py` | 115 文件树、整理记录、上传记录、媒体信息/指纹缓存 |
| `subscription` | `watchlist_processor.py`、订阅/洗版/演员模块 | 订阅规则、追剧/洗版状态、订阅助手状态 |
| `collection` | TMDb/自建合集、封面模块 | `collections_info`、`custom_collections`、合集呈现 |
| `sharing` | shared routes/handlers/tasks、HDHive/TG | `shared_*`、贡献点 Ledger、虚拟导入 |
| `users` | user portal/management、播放事件 | 用户媒体状态、收藏、用户模板 |
| `operations` | task/log/update/database admin | 任务运行、日志、健康、迁移、系统更新 |

`media_metadata` 当前同时保存媒体、订阅、追剧和洗版字段。第一轮不拆表：

- `media` 是该表的物理 Repository 所有者。
- 其他领域通过明确的 Query/Command Port 访问自己负责的字段。
- 等所有调用都经过 Port 后，再用独立 ADR 决定是否拆表；不把拆表绑进本次兼容迁移。

## 8. 兼容性合同

### 8.1 HTTP 合同

每个现有端点必须记录并比较：

- Method、Path、Query、Path 参数、Header、Cookie 和 Body。
- 缺省值、空值、布尔值、日期、分页和排序语义。
- HTTP 状态码、Content-Type、JSON 字段、字段类型和 envelope。
- 成功、权限失败、校验失败、外部超时、记录不存在和任务冲突。
- 流式响应、文件响应、图片代理和 WebSocket 行为。

新实现不得因为 Pydantic 默认校验而擅自把旧的 `200 + status=error` 改成 `422`。兼容期由 `response_contract.py` 明确映射；未来纠正协议必须另开 API 版本。

### 8.2 Session 与权限合同

- 继续使用当前 Secret Key 文件来源和 Cookie 名称。
- 保留 `emby_user_id`、`emby_username`、`emby_is_admin`、`is_setup_mode` 等 Session Key。
- 管理员权限仍以数据库实时检查为准，不信任缓存的 `emby_is_admin`。
- 新 `session_codec.py` 必须与 Flask `SecureCookieSessionInterface` 做双向 fixture 测试。
- 迁移期兼容宿主中的 FastAPI 与 Flask fallback 必须共享 Session；全新环境不接收旧环境 Cookie，用户在首次启用后重新登录属于预期行为。

### 8.3 数据库合同

- 第一阶段 Alembic baseline 与当前 39 表 schema diff 必须为空。
- 现有主键、唯一约束、JSONB 形状、时间语义和空值语义必须保留。
- 迁移初期只允许新增表、列和索引；删除/改类型使用 expand-contract 两阶段迁移。
- 每个 Use Case 只有一个明确事务边界。
- 外部调用不长时间占用数据库事务。
- 贡献点、订阅额度、删除和共享状态必须使用原子更新或行锁，禁止读后写竞态。

### 8.4 配置合同

- 保留环境变量、`config.ini`、数据库动态配置和默认值的现有优先级。
- 保存配置后继续触发相同的 Processor/Monitor/Scheduler 重载结果。
- 新实现改为发布 `config.updated`，由各进程原子替换配置快照。
- 配置 round-trip 前后未知键不得丢失。
- Secret 不进入日志、fixture、OpenAPI 示例或 Git。

### 8.5 任务合同

- 保留全部 50 个任务 Key 和现有前端可见性。
- 保留高频/低频任务链、顺序、最大运行时间和停止信号语义。
- 每次运行生成稳定 `job_id`，状态持久化到 PostgreSQL。
- Celery 使用 at-least-once 投递；所有可能重复执行的副作用必须有幂等键。
- Worker 崩溃后任务必须恢复、重试或明确失败，禁止永久显示“运行中”。
- 取消采用协作式取消；数据库中记录 `cancel_requested_at`，任务在安全点检查。

### 8.6 Bridge 与 Emby 合同

至少冻结以下已确认端点及其 PickCode/SHA1 双路径：

- `GET /api/emby/metadata`
- `POST /api/emby/metadata/backfill`
- `POST /api/emby/metadata/backfill/item`
- `POST /api/emby/metadata/backfill/items`
- `POST /api/emby/metadata/backfill/library`
- `POST /api/emby/metadata/images/refresh`
- `GET /api/emby/metadata/images/search`
- `POST /api/emby/metadata/images/sync`
- `POST /api/emby/events`
- `GET /api/emby/plugin-update`
- `GET /api/p115/mediainfo/<pick_code>`
- `GET /api/p115/mediainfo/sha1/<sha1>`
- `GET /api/p115/mediainfo/<pick_code>/metadata`
- `GET /api/p115/mediainfo/sha1/<sha1>/metadata`
- `POST .../metadata/backfill`
- `POST .../intro-sync`
- `POST .../item-ready`
- `POST .../deep-delete/prepare`
- `POST .../deep-delete/commit`

必须覆盖 Movie、Series、Season、Episode、STRM、实体媒体、ItemID 重绑、图片策略、媒体流、章节和深度删除流程。

### 8.7 前端合同

- 保留现有 23 个主要页面路径和 Router Guard 行为。
- `/login`、`/setup`、邀请注册、管理员页面和普通用户页面权限不变。
- 后端迁移期间现有 JavaScript 前端必须无需修改即可工作。
- TypeScript 迁移按页面进行，不与对应后端业务迁移捆绑提交。

## 9. 执行阶段总览

| 阶段 | 目标 | 主要产物 | 依赖 | 相对工作量 |
| --- | --- | --- | --- | --- |
| P0 | 行为基线和安全护栏 | Contract Pack、DB baseline、测试环境 | 无 | M |
| P1 | 新明文骨架 | `main.py`、FastAPI、Flask fallback | P0 | M |
| P2 | 配置、DB、DI 平台 | Settings、SQLAlchemy、Alembic、Container | P1 | L |
| P3 | System/Auth/Operations | 登录、配置、健康、日志、管理接口 | P2 | L |
| P4 | 持久任务与运行角色 | Celery、Redis、Coordinator、Job Store | P2 | XL |
| P5 | Media/Metadata/Bridge | 中央媒体模型和 Bridge 全合同 | P3/P4 | XL |
| P6 | 115/入库/监控/播放 | 115 全链和媒体入库 | P5 | XL |
| P7 | 订阅/追剧/演员/洗版 | 订阅域全部任务和页面接口 | P5/P6 | XL |
| P8 | 合集/封面/用户/发现 | 次级业务域 | P5 | L |
| P9 | 共享/积分/反代/TG/HDHive | 高风险外部联动域 | P4/P6/P7 | XL |
| P10 | 前端 TypeScript 化 | Typed Client、Vue Query、55 个页面/组件 | P3-P9 稳定契约 | XL |
| P11 | 清理和非功能验收 | 移除 legacy、性能/可靠性报告 | P3-P10 | L |
| P12 | 新环境部署、启用和观察 | Greenfield Deployment、Activation、Rebuild Pack | P11 | L |

`S/M/L/XL` 只表示相对规模，不作为日历承诺。外部系统可用性和行为基线完整度决定实际周期。

### 9.1 当前 Blueprint/Route 迁移归属

| 当前路由文件或 Blueprint | 目标阶段 | 目标领域 | 说明 |
| --- | --- | --- | --- |
| `routes/unified_auth.py` | P3 | `identity` | Session、setup、登录和重新授权 |
| `routes/system.py` | P3/P4 | `system`、`operations` | 配置/健康在 P3，任务和运行服务控制在 P4 |
| `routes/logs.py` | P3 | `operations` | 日志读取、搜索和下载 |
| `routes/database_admin.py` | P3/P11 | `operations` | 查询/安全维护先迁移，legacy DDL 在 P11 移除 |
| `routes/tasks.py` | P3/P4 | `operations` | 查询合同在 P3，执行和取消在 P4 |
| `routes/media.py` 的 `media_api_bp` | P5 | `media` | 媒体查询、编辑、图片和自动标签 |
| `routes/media.py` 的 `media_proxy_bp` | P5 | `media` | 图片代理和缓存响应 |
| `routes/actions.py` | P5/P8 | `media`、`operations` | 媒体动作随 P5，其余批处理随 P8 |
| `routes/webhook.py` | P5/P7/P9 | `ingestion`、`subscription`、`sharing` | 按事件类型拆 Router，旧 Blueprint 最后移除 |
| `routes/p115.py` | P6 | `p115` | 认证、目录、整理、媒体信息和播放 |
| `routes/watchlist.py` | P7 | `subscription` | 追剧状态和季集维护 |
| `routes/actor_subscriptions.py` | P7 | `people`、`subscription` | 演员追踪和作品订阅 |
| `routes/subscription.py` | P7/P9 | `subscription`、`sharing` | 普通订阅在 P7，影巢/共享联动在 P9 |
| `routes/resubscribe.py` | P7 | `subscription` | 补订阅与洗版 |
| `routes/media_cleanup.py` | P7 | `subscription`、`operations` | 扫描、忽略、删除计划 |
| `routes/tmdb_collections.py` | P8 | `collection` | 原生 TMDb 合集 |
| `routes/custom_collections.py` | P8/P9 | `collection`、`sharing` | 合集规则在 P8，虚拟代理联动在 P9 |
| `routes/cover_generator_config.py` | P8 | `collection` | 封面配置、预览和生成 |
| `routes/user_management.py` | P8 | `users`、`identity` | 用户同步、邀请和权限管理 |
| `routes/user_portal.py` | P8 | `users` | 普通用户门户、收藏和状态 |
| `routes/discover.py` | P8 | `media`、`users` | 搜索、推荐和每日主题 |
| `routes/shared_resource.py` | P9 | `sharing` | 共享、贡献点、虚拟资源和中心交互 |
| `reverse_proxy.py` 的 `proxy_app` | P9 | `sharing`、`users` | 独立 ASGI Proxy；包含 HTTP/WebSocket/播放并发 |

P0 必须生成 `verification/contracts/route-ownership.yaml`，为每个运行时路由记录：

- 唯一合同 ID。
- 当前 Method/Path/Endpoint。
- 所属领域和目标阶段。
- 状态：`legacy`、`new`、`retired-approved`。
- Golden Master fixture 和验证负责人。

CI 必须拒绝未登记路由、重复活动实现以及没有批准记录的删除。任务、配置和数据库表采用同样的 Manifest 机制，避免“看起来迁完了”但实际遗漏隐蔽入口。

## 10. 分阶段执行步骤

### P0：行为基线和安全护栏

**目标：** 在修改运行结构前，先把“什么叫行为没有变”固化为可重复验证。

#### 工作项

- [ ] 创建 `codex/refactor-mainline` 工作分支；不直接在 `main` 大规模开发。
- [ ] 建立 `verification/`，永久测试不放入项目约定的临时 `tests/`。
- [ ] 新增 `scripts/refactor/export_*`，导出并脱敏以下资产：
  - [ ] Flask 路由清单。
  - [ ] 50 个任务定义及 processor type。
  - [ ] 配置键、默认值、来源和动态/重启级别。
  - [ ] PostgreSQL schema-only、表/列/索引/约束清单。
  - [ ] 前端路由和页面权限清单。
  - [ ] Bridge/Emby/115 外部合同清单。
- [ ] 为关键 API 建立 Golden Master fixture；真实 Token、Cookie、URL 和个人数据全部替换。
- [ ] 为数据库建立最小匿名 fixture，覆盖 Movie/Series/Season/Episode、订阅、共享、用户和任务。
- [ ] 记录旧系统基准：启动、健康、常用查询 p50/p95、任务投递、内存、外部超时行为。
- [ ] 从源码创建可丢弃的旧版参考环境，并准备目标新环境：不同配置目录、数据库、端口、媒体目录和 Webhook Stub；不使用当前运行环境。

#### 基线命令

```powershell
python -m compileall -q -x "node_modules|frontend|local_data" .
python -m unittest handler.p115_media_recognition handler.tg_media_candidate_flow
npm --prefix frontend ci
npm --prefix frontend run build
npm run docs:build
```

连接隔离测试库后执行：

```powershell
pg_dump --schema-only --no-owner --no-privileges `
  --dbname "$env:ETK_BASELINE_DATABASE_URL" `
  --file "verification/contracts/db/legacy-schema.sql"

python scripts/refactor/export_contracts.py `
  --base-url "$env:ETK_BASELINE_BASE_URL" `
  --output "verification/contracts/http"
```

#### 退出门槛

- Contract Pack 可在无生产凭据情况下重复执行。
- 所有 fixture 完成 Secret/个人数据扫描。
- 当前前端能够在可丢弃的旧版参考环境运行关键流程。
- 明确列出无法自动捕获的外部合同及人工验证步骤。

#### 停止条件

- 无法获得隔离数据库或隔离媒体目录。
- Contract Pack 包含真实 Secret、Cookie、用户数据或共享中心私密响应。
- 旧系统自身关键流程在基准环境不能稳定复现。

### P1：建立新明文主线和兼容宿主

**目标：** 新入口能够运行现有业务；目标 Greenfield 环境尚不对外部署。

#### 工作项

- [ ] 新增 `pyproject.toml`、`src/etk/` 和顶层 `main.py`。
- [ ] 建立无副作用的 FastAPI `app_factory`。
- [ ] 根据当前代码、Docker 行为和历史明文快照重建 `compatibility/flask_app.py`。
- [ ] 将尚未迁移的 22 个 Blueprint 注册到兼容 Flask App。
- [ ] 使用 Starlette `WSGIMiddleware` 把兼容 App 挂载为最后匹配的 fallback。
- [ ] 新 FastAPI Router 始终排在 fallback 前；同一路径只能有一个活动实现。
- [ ] 新建 `RuntimeSupervisor`，接管配置重载、Processor 构造和关闭顺序。
- [ ] 将 `routes/system.py`、`routes/unified_auth.py` 两处导入改到新 Supervisor。
- [ ] 提供 `main.py generate-nginx-config`、`main.py api`、`main.py proxy` 命令。
- [ ] 新入口使用测试端口；目标 Dockerfile、Compose 和 entrypoint 在隔离环境验证，不修改当前运行环境。

#### 验证

```powershell
uv sync --frozen
uv run python main.py --help
uv run python main.py api --host 127.0.0.1 --port 15257
uv run pytest verification/contracts -q
```

静态边界验证：

```powershell
rg -n "from web_app import|import web_app|pyarmor_runtime" `
  main.py src routes
```

#### 退出门槛

- 新进程启动时没有导入 `web_app` 或 PyArmor runtime。
- 兼容 App 的路由清单与基线一致。
- 健康、登录、配置读取、SPA fallback 和静态资源可用。
- 新旧基准环境 Contract Diff 没有未批准差异。

#### 回退

目标新环境尚未启用；删除新入口分支和可丢弃测试环境即可回退，不涉及当前运行环境。

### P2：配置、数据库和依赖装配平台

**目标：** 建立后续所有领域统一使用的平台能力。

#### 工作项

- [ ] 用 Pydantic Settings 描述配置，但通过 Legacy Config Adapter 保留原优先级和格式。
- [ ] 建立 `AppContainer`，显式装配 Settings、DB、Repository、Client 和 Use Case。
- [ ] 新代码禁止导入 `extensions`；旧模块由 `legacy_registry` 临时注入。
- [ ] 建立 SQLAlchemy 2 Engine、Session、UnitOfWork 和 Repository 基类。
- [ ] 根据当前源码中的 39 张表建立 Alembic baseline；可丢弃参考库只用于 Schema 对照，不读取当前运行数据库。
- [ ] 全新的 PostgreSQL 18 空数据库必须能通过 `alembic upgrade head` 得到与兼容基线一致的 Schema。
- [ ] `docker/compose.refactor.yml` 明确使用 `postgres:18-alpine`，并在发布时按镜像 Digest 固定。
- [ ] 把启动建表和历史迁移整理为版本化账本；暂不删除旧 `init_db()`。
- [ ] 定义事务规范、锁规范、重试规范和 Repository 返回类型。
- [ ] 建立结构化日志、Trace ID、敏感字段过滤和 Prometheus 基础指标。

#### 验证

```powershell
uv run alembic upgrade head
uv run python scripts/refactor/compare_schema.py `
  verification/contracts/db/legacy-schema.sql `
  "$env:ETK_NEW_DATABASE_URL"
uv run pytest verification/unit/platform verification/integration/platform -q
uv run pyright src/etk/platform src/etk/bootstrap
```

#### 退出门槛

- 源码 Schema、可丢弃参考库与 Alembic 定义没有未解释的业务语义差异。
- PostgreSQL 18 新空库的升级结果与兼容基线一致。
- 目标 Compose、Testcontainers 和 CI 均实际报告 PostgreSQL Server Major Version 为 18。
- 配置 round-trip 不丢未知键，重载结果与旧实现一致。
- API Factory 在不连接 Emby/115 的情况下可以被测试导入。
- DB 连接池、事务、回滚和 shutdown 有集成验证。

### P3：System、Identity 与 Operations

**目标：** 先迁移依赖最广、业务副作用相对可控的平台接口。

#### 范围

- `routes/unified_auth.py`
- `routes/system.py`
- `routes/logs.py`
- `routes/database_admin.py` 中只读和维护入口
- `routes/tasks.py` 的查询接口，任务执行在 P4 完成
- `config_manager.py`、`extensions.py` 中的权限和配置职责

#### 工作项

- [ ] 建立 Session 兼容 Middleware 和权限 Policy。
- [ ] 迁移 setup、login、logout、auth status、reauthorize。
- [ ] 迁移配置读取、保存、连接测试、健康和版本状态。
- [ ] 迁移日志查询、搜索和下载，保留现有格式。
- [ ] 迁移数据库统计和只读管理查询。
- [ ] 为每个端点定义 Pydantic DTO，但使用兼容响应映射。
- [ ] 生成并冻结第一版 OpenAPI；对外 Path 不增加 `/v2`。

#### 退出门槛

- 旧 Cookie 可被新 API 读取，新 Cookie 可被兼容 Flask 路由读取。
- 管理员、普通用户、未登录、setup mode 的权限矩阵全部通过。
- 配置保存失败时不会留下半写配置或启动重复服务。
- 当前前端无需修改即可使用全部迁移接口。

### P4：持久任务、调度和运行角色

**目标：** 从 API 进程剥离所有长任务和常驻后台服务。

#### 工作项

- [ ] 增加 `job_runs`、`job_attempts`、`outbox_events` 和幂等记录表。
- [ ] 建立 Celery App、Redis Broker、任务序列化规则和队列分类。
- [ ] 任务参数只允许 JSON 可序列化 DTO，不传 Processor 实例或数据库连接。
- [ ] 迁移任务状态、进度、日志、取消、超时、重试和崩溃恢复。
- [ ] 建立 Coordinator：读取现有 Cron 配置并投递任务，不直接执行任务。
- [ ] 将 Watchdog、115 daemon 和其他常驻监听迁到 Coordinator/Agent 角色。
- [ ] 使用 PostgreSQL advisory lock 保证 Scheduler、Monitor、daemon 单例。
- [ ] 保留全部 50 个任务 Key，并建立 Legacy Task → New Job 映射表。
- [ ] 任务链改为显式 Workflow；每个子任务有独立状态和幂等键。
- [ ] API 只创建/查询/取消任务，不运行任务函数。

#### 可靠性验收

- Worker 执行中 `kill -9` 后，任务能按策略重试或明确失败。
- Redis 短时不可用不会损坏业务状态；恢复后可继续投递。
- 重复投递同一幂等任务不会重复扣额度、重复分享或重复删除。
- Coordinator 同时启动两个实例时只有一个持有单例锁。
- 取消信号在安全点生效，数据库事务不处于半提交状态。
- 50 个任务 Key、可见性和任务帮助文本与基线一致。

### P5：Media、Metadata、Image 与 Bridge

**目标：** 迁移中心媒体模型和最关键外部插件合同。

#### 范围

- `core_processor.py`
- `routes/media.py`
- `routes/webhook.py` 中 Bridge/Emby 元数据事件
- `database/media_db.py`、`metadata_provider_db.py`、`image_policy_db.py`
- `handler/tmdb.py`、`douban.py`、`emby.py` 的媒体相关 Client
- 图片缓存、媒体信息和 ItemID 绑定

#### 应用用例

- `ResolveMediaIdentity`
- `IngestMediaMetadata`
- `BackfillMediaMetadata`
- `BindEmbyItem`
- `ProvideBridgeMetadata`
- `ProvideBridgeImages`
- `SyncImagePolicy`
- `RestoreMediaInfo`
- `HandleEmbyEvent`
- `PrepareDeepDelete` / `CommitDeepDelete`

#### 工作项

- [ ] 先建立只读 Query Repository，再迁移写路径。
- [ ] 把 TMDb 聚合、翻译、人物和图片选择拆成独立 Policy。
- [ ] 把数据库写入和 Emby 回写拆成两个阶段，使用 Outbox/幂等键处理可靠副作用。
- [ ] 迁移 PickCode、SHA1、路径、TMDb 和 Emby ItemID 的身份解析。
- [ ] 迁移 Movie/Series/Season/Episode 的层级规则。
- [ ] 迁移图片策略、持久化图片仓库、截图和缓存清理。
- [ ] 迁移媒体信息、章节、intro-sync、item-ready 和重绑定。
- [ ] 对 Bridge 全端点执行 Golden Master Diff。

#### 退出门槛

- 四种媒体类型的元数据 JSON 与旧版合同一致。
- Season 不被 Episode 集号污染；Episode 父层级关系正确。
- 一个 STRM 和一个实体媒体测试样本完成身份识别、入库和最终刷新。
- ItemID 重绑不触发不必要的 TMDb、翻译或入库任务。
- 图片策略变化、手动选图、替换现有图像和无引用清理行为一致。
- 失败重试不会重复写入资产、重复通知或错误删除媒体。

### P6：115、入库、监控和播放

**目标：** 迁移代码量最大、外部副作用最多的 115 与入库链。

#### 子切片顺序

1. 115 认证与只读目录浏览。
2. 文件系统缓存和目录同步。
3. 媒体识别、命名和整理计划 Dry Run。
4. 整理执行、记录和手动纠错。
5. STRM/字幕生成与全量同步。
6. 媒体信息、ISO、ffprobe 和指纹缓存。
7. Watchdog 新增/移动/删除批处理。
8. 上传监控和 Life Event daemon。
9. 直链、播放池、临时文件、复制播放和回收。
10. 片头/片尾检测与章节回写。

#### 安全规则

- 所有移动、重命名、删除、转存先提供可审计 Dry Run Plan。
- 115 写操作使用业务幂等键和结果回读，不以 HTTP 200 作为成功证明。
- 文件稳定性检测和批处理顺序必须保留。
- STRM 内容、容器内路径和 PickCode/SHA1 规则不得改变。
- 播放临时文件必须有租约、所有者和回收状态，不使用纯进程内字典作为事实来源。
- ffmpeg/ffprobe 使用受控子进程、超时、输出上限和取消。

#### 退出门槛

- 认证、扫码、Cookie 更新和失效恢复合同一致。
- 一个测试电影和一季测试剧集完成识别、整理、STRM、入库、媒体信息和播放。
- 重复事件不会重复整理；删除事件不会误删相邻媒体。
- Worker/Coordinator 重启后上传、播放临时状态和 Monitor 队列可恢复。
- 115 API 限流、超时和部分失败有确定重试/停止规则。

### P7：订阅、追剧、演员和洗版

**目标：** 迁移围绕 `media_metadata` 的状态型业务。

#### 范围

- `watchlist_processor.py`
- `actor_subscription_processor.py`
- `routes/watchlist.py`、`actor_subscriptions.py`、`subscription.py`、`resubscribe.py`、`media_cleanup.py`
- `services/subscribe_assistant/`
- MoviePilot 订阅和相关任务

#### 当前实施状态

- `13c6d32e` 已提交 subscription runtime、Actor/Watchlist native routes、Cleanup/Resubscribe
  foundations、PostgreSQL operation state、MoviePilot provider execution、process wiring 和 P7
  verification assets；P7 仍为 `DOING`。
- 后续按三个批次收口：`P7-FIX`、`P7-HTTP/STATE`、`P7-RUNTIME`。
- 已通过的 Actor、Watchlist 和 operation runtime 不重新探索或重做，除非测试或 review
  提供新的确定缺陷。
- Manifest/OpenAPI 可在阶段批次内暂时 stale，但必须记录为 pending，并在 P7 阶段末统一更新和
  `--check`。

#### 工作项

- [ ] 把追剧状态转换为显式 State Machine。
- [ ] 把订阅来源、额度、补订阅和洗版决策转换为领域 Policy。
- [ ] 为演员订阅、人物映射和作品筛选建立独立领域服务。
- [ ] 订阅助手使用持久 Job/State，不依赖单进程内存。
- [ ] MoviePilot Client 统一 timeout、错误映射和幂等提交。
- [ ] 对订阅额度、共享积分和自动删除建立事务不变量测试。

#### 退出门槛

- 连载、暂停、完结、强制完结、缺季和缺集场景状态一致。
- 订阅重复回调不会重复提交或重复扣额度。
- 洗版规则相同输入得到相同优先级和决策。
- 演员别名、角色翻译和作品筛选结果与基线一致。
- 订阅助手超时、完成快照和过期清理可在 Worker 重启后继续。

### P8：合集、封面、用户、发现和次级管理域

**目标：** 迁移与中心媒体模型相关但风险相对独立的功能。

#### 范围

- TMDb 合集、自建合集和集合封面。
- 用户管理、用户门户、播放统计和收藏。
- Discover、自动标签、日志复核、封面生成。
- 系统更新和数据库维护任务。

#### 退出门槛

- `ImportCollections`、`MinCollectionItems` 和合集激活语义不变。
- 自建合集虚拟 ID、排序、分页和可见用户规则一致。
- 用户权限、播放状态、收藏和统计结果一致。
- 封面生成像素尺寸、字体、样式和缓存结果通过 fixture/图像差异阈值。
- 系统更新流程不会在 API 请求线程内执行 Docker 更新。

### P9：共享中心、贡献点、反向代理、Telegram 与 HDHive

**目标：** 最后迁移外部依赖最多、涉及贡献点和播放代理的高风险域。

#### 工作项

- [ ] 将共享登记、转存、秒传、求共享和订阅消费拆为明确 Use Case。
- [ ] 贡献点以 Ledger 为唯一事实来源，所有扣减/补偿使用数据库事务和幂等键。
- [ ] 中心端 Client 统一签名、timeout、重试、响应校验和故障熔断。
- [ ] Telegram Bot/UserBot 使用持久会话状态和独立 Worker 队列。
- [ ] HDHive 授权、签到和资源结果使用独立 Adapter。
- [ ] 将 `reverse_proxy.py` 迁为独立 ASGI Proxy 进程。
- [ ] HTTP 和 WebSocket 代理分别建立合同与压力测试。
- [ ] 虚拟库、虚拟播放、并发限制和临时资源回收使用持久租约。

#### Local Code Complete 门槛

- 同一事件重复投递不会重复扣减或奖励贡献点。
- 共享中心部分成功/超时可以安全重试或补偿。
- 虚拟库响应、ID、排序、分页和客户端兼容性通过 Golden Master。
- HTTP/WebSocket Proxy、起播/停止状态、并发租约和资源回收使用隔离测试服务及 Fake/Stub
  通过合同、并发和压力验证。
- Telegram/HDHive 离线不会拖垮 API 或主任务队列。

本地门禁只使用 Fake/Stub，通过后将 P9 标记为 `LOCAL_DONE`。真实 Emby 起播、停止、
WebSocket、并发限制和资源回收属于 Live Acceptance，保持 `LIVE/HUMAN_PENDING`，不得用本地
Fake/Stub、HTTP 200 或健康检查替代。

### P10：前端 TypeScript 与统一 API 层

**目标：** 保留现有产品交互，移除散落 API 和弱类型状态。

#### 工作项

- [ ] 启用 TypeScript strict、`vue-tsc --noEmit` 和 ESLint/Ruff 等价质量门槛。
- [ ] P3-P9 后端合同冻结后只从 OpenAPI 生成一次 `frontend/src/api/generated/`；禁止手改生成物，
  只有后端合同变化时才重新打开合同阶段并重新生成。
- [ ] 用 openapi-fetch 封装兼容 envelope、上传、下载和流式请求。
- [ ] 用 Vue Query 管理查询、缓存、失效和 Mutation；Pinia 只保留 auth/UI 状态。
- [ ] 按路由逐页把 55 个 Vue 文件迁移为 `<script setup lang="ts">`。
- [ ] 保留现有 Router Path、Guard、响应式布局和组件库视觉。
- [ ] 为登录、配置、任务、整理、订阅、共享、用户中心建立 E2E。
- [ ] 删除最后一处直接 `axios.*`/裸 `/api/` 字符串前，先通过静态扫描门槛。

#### 验证

```powershell
npm --prefix frontend ci
npm --prefix frontend run typecheck
npm --prefix frontend run test:unit
npm --prefix frontend run build
npm --prefix frontend run test:e2e
```

#### 退出门槛

- 现有页面路径和权限守卫全部通过。
- 生产构建无 TypeScript error。
- API URL 只存在于生成 Client 或明确的流式/代理 Adapter。
- 关键工作流 E2E 在新后端通过。
- 浏览器 Console 没有未处理 Promise、模块 MIME 或 hydration 错误。

### P11：移除兼容层与非功能验收

**目标：** 新主线完全独立，技术债没有被搬到新的 `legacy` 目录长期存活。

#### 工作项

- [ ] 先使 Route/Task/Table/Config/Frontend ownership 全部达到 100%，生成 legacy 删除清单、
  替代位置、依赖/import 扫描和镜像内容清单。
- [ ] 在上述 pre-removal 证据完整后，一次受控移除 WSGI fallback 和其他 active legacy runtime；
  不删除仓库中的 `web_app.py`。
- [ ] 移除活动代码对 Flask、gevent、psycopg2、requests 和 extensions 的依赖。
- [ ] 移除旧 Processor、TaskManager、SchedulerManager 和启动建表调用。
- [ ] 旧文件删除前逐个在迁移清单标记替代位置和验证证据。
- [ ] 从新 Docker 镜像排除 `web_app.py` 和 PyArmor runtime。
- [ ] 执行完整安全、性能、可靠性和资源使用验证。
- [ ] 生成 SBOM、依赖漏洞报告和 License 清单。

#### 非功能门槛

- 健康接口 p95 ≤ 100ms（隔离本地网络）。
- 纯数据库读取 API p95 不高于旧基线 110%，目标 ≤ 250ms。
- 创建后台任务的 API p95 ≤ 500ms，不等待任务执行。
- API 进程不执行超过 500ms 的 CPU/阻塞工作。
- 外部 Client 均有 connect/read/total timeout、有限重试和并发上限。
- Worker 强制终止后没有永久 RUNNING 任务。
- 所有错误日志包含 `trace_id`；任务日志包含 `job_id/task_key`。
- 关键 Application/Domain 代码分支覆盖率 90%，改动行覆盖率至少 85%。
- Bridge、贡献点、删除、任务幂等和认证合同必须 100% 通过，不以覆盖率替代。

### P12：全新环境部署、启用和观察

**目标：** 从空目录和空 Volume 创建独立的新环境；全部验收通过后，一次启用外部访问和写入。当前环境不参与部署、数据迁移或回退。

#### 12.1 可销毁环境演练

本节必须由只允许作用于 disposable local Greenfield 的安全自动化 harness 执行：

```text
create -> migrate -> verify -> capture evidence -> destroy
       -> rebuild from empty -> verify -> failure-rebuild drill
```

在任何销毁动作前，harness 必须断言目标是允许根目录下的绝对路径，Compose Project 名称隔离且
可识别，并从解析后的 Compose 配置及 labels 核对容器、网络和 Volume 全部属于该新环境；发现旧
`/config`、旧数据库路径、生产别名或未知 Volume 时必须拒绝执行。证据包至少保存镜像 Digest、
PostgreSQL Server Version、Alembic revision、配置摘要、日志和测试报告。

1. 创建与目标环境同拓扑、但名称和端口完全隔离的演练 Compose Project。
2. 为 `/config`、PostgreSQL 18、Redis、媒体测试目录和静态文件创建全新路径/Volume。
3. 启动 PostgreSQL 18，运行 `alembic upgrade head`，再启动 API、Worker、Coordinator、Proxy 和 Nginx。
4. 默认关闭真实 Scheduler、Watchdog 和外部写入，使用 Stub 或明确的测试账户完成首次配置。
5. 加载脱敏 fixture，执行 Contract Diff、数据库不变量、任务恢复和关键 E2E。
6. 启用测试 Webhook、调度和一部测试媒体 Canary，验证完整闭环。
7. 停止演练环境，核对解析后的绝对路径和 Volume 名称后，销毁整个演练 Project 及其可丢弃 Volume。
8. 只依赖 Compose、Alembic、配置步骤和安全凭据来源，从空状态重新创建并再次通过 Smoke。
9. 模拟启用失败：关闭外部入口、停止写入、保存证据、清空新环境并再次重建；不启动当前旧环境。
10. 保存演练报告、镜像 Digest、PostgreSQL Server Version、Schema revision、配置清单和验证证据。

#### 12.2 新环境启用前门槛

- [ ] P0-P11 全部退出门槛通过。
- [ ] 目标主机、Compose Project 名称、端口、网络、挂载路径和 Volume 名称已核对。
- [ ] 新 `/config`、PostgreSQL 18 和 Redis 均为新建目标，不复用当前环境内容。
- [ ] 新镜像按 Digest 固定，禁止使用未锁定的 `latest`。
- [ ] PostgreSQL 18 空库初始化、整套环境销毁重建和 fixture 验收已完整演练。
- [ ] Emby、115、TMDb、MoviePilot、Bridge、共享中心、Telegram 和 HDHive 均有明确的新授权来源和回调地址。
- [ ] 当前旧实例若仍然存在，已确认不会继续消费相同 Webhook、调度、监控目录或共享中心事件；无需记录它的容器、镜像或 Volume 细节。
- [ ] 观察指标 Dashboard、错误告警和新环境停止命令可用。
- [ ] 明确新环境启用负责人、停止条件和允许的初始化写入范围。

#### 12.3 全新环境部署与启用步骤

1. 在目标位置创建全新的 Compose Project、`/config`、PostgreSQL 18、Redis、日志和媒体挂载目录。
2. 核对所有解析后的绝对路径均属于目标新环境；不读取、挂载或覆盖当前环境目录。
3. 通过安全来源写入新环境 Secret；不复制旧 `.env`、Cookie、Session 或数据库状态。
4. 仅启动 PostgreSQL 18 和 Redis，断言 `SHOW server_version_num` 为 18.x。
5. 运行 `alembic upgrade head`，验证 39 张兼容表、revision、索引、约束、数据库用户和时区设置。
6. 启动 API、Worker、Coordinator、Proxy 和 Nginx，但保持 Scheduler、Watchdog、Webhook 和外部写入关闭。
7. 完成首次配置：管理员、Emby、115、TMDb 以及按需使用的 MoviePilot、共享中心、Telegram 和 HDHive 授权。
8. 执行只读 Smoke：登录、配置、媒体查询、任务清单、订阅清单和共享清单；空列表必须是预期结果而不是查询失败。
9. 启动必要的初始化同步，验证任务持久状态、进度、幂等和失败恢复。
10. 执行一部明确标记的测试媒体完整入库 Canary。
11. 验证新数据库记录、Emby 项目、Bridge 回调、媒体流、图片、任务状态和日志。
12. 再次确认没有其他实例消费相同外部事件，然后启用新环境的 Webhook、Scheduler、Watchdog 和 daemon。
13. 开放新环境的正式访问端口或域名，开始稳定观察。

#### 12.4 启用停止条件

出现任一情况立即关闭新环境外部入口和后台写入：

- 登录/权限出现越权或大面积失效。
- PostgreSQL 版本、空库 Schema、初始化结果或数据库不变量与演练不符。
- 贡献点、额度、订阅或共享记录出现重复扣减/重复写入。
- Bridge 元数据、媒体流、图片或 ItemReady 连续失败。
- 发生非预期媒体删除、115 移动、重复整理或错误转存。
- Worker 重复执行不可幂等任务。
- 发现其他实例仍在消费相同 Webhook、调度、目录监控或共享中心事件。
- API/Proxy 错误率超过基线阈值且无法在观察窗口内解释。

#### 12.5 启用失败处理

1. 关闭新环境入口，禁用 Scheduler、Watchdog、Webhook、daemon 和 Worker 新任务消费。
2. 等待安全中的任务结束或发出协作式取消，停止 API、Proxy、Coordinator 和 Worker。
3. 保存新系统日志、任务状态、数据库 revision、Server Version 和失败请求证据。
4. 对已经发生的 Emby、115、共享中心等外部副作用逐项对账，禁止盲目重放。
5. 根据根因选择重新部署同一新环境，或核对目标路径后销毁新 PostgreSQL 18/Redis/`/config` 并从空状态重建。
6. 重新通过 Smoke 和 Canary 后才恢复外部入口及后台写入。
7. 不启动、不恢复、不依赖当前旧环境。后续新版本发布时，只有 Schema 明确向后兼容才允许回退到上一版新镜像。

#### 12.6 稳定观察结束

- 观察期长度由真实任务周期决定，至少覆盖一次高频任务链、一次低频任务链、一次监控入库、一次 Bridge 刷新和一次共享维护。
- 新环境必须能够仅凭锁定镜像、Compose、Alembic、配置清单和安全凭据来源从空状态恢复。
- 当前旧环境不属于完成条件。停止或删除旧容器、Volume、目录和镜像缓存是独立清理任务，执行时另行点名确认。

## 11. 验证体系

### 11.1 测试分层

| 层级 | 验证对象 | 是否连接外部系统 |
| --- | --- | --- |
| Unit | Domain Policy、Value Object、状态机、DTO | 否 |
| Application | Use Case、事务、幂等、补偿 | 使用 Fake Port |
| Repository Integration | PostgreSQL SQL、锁、索引、事务 | Testcontainers PostgreSQL |
| Adapter Contract | Emby/115/TMDb/MoviePilot/Center 请求响应 | 录制的脱敏 fixture 或专用测试服务 |
| HTTP Contract | 新旧 Path、状态码、JSON、Cookie | 两套隔离应用 |
| Workflow Integration | Task、Scheduler、Worker、Redis、Outbox | 本地 Compose |
| E2E | 浏览器关键工作流 | 隔离后端与测试账户 |
| Live Canary | 真实 Emby/115/Bridge 最小闭环 | 仅明确授权的测试对象 |

### 11.2 必须维护的数据库不变量

- `media_metadata` 的 `(tmdb_id, item_type)` 唯一且层级字段合法。
- 同一实际版本的 PickCode/SHA1/Emby ItemID 不产生错误跨媒体绑定。
- `p115_mediainfo_cache` 的格式化媒体流和 RAW 身份不被并发覆盖。
- 共享贡献点 Ledger 总和与 Snapshot 一致。
- 订阅额度不会因为重试降为负数或重复扣减。
- Deep Delete 的 prepare/commit Token 不能跨项目或重复提交。
- Job 状态只能按允许的状态机转换。
- Outbox Event 有唯一幂等键且只确认已完成的外部副作用。

### 11.3 合并前统一命令

```powershell
uv sync --frozen
uv run ruff check src verification scripts
uv run ruff format --check src verification scripts
uv run pyright
uv run pytest verification/unit verification/integration verification/contracts -q

npm --prefix frontend ci
npm --prefix frontend run typecheck
npm --prefix frontend run test:unit
npm --prefix frontend run build

docker compose -f docker/compose.refactor.yml config -q
docker compose -f docker/compose.refactor.yml up -d --build
uv run pytest verification/e2e/api -q
docker compose -f docker/compose.refactor.yml down

npm run docs:build
```

任何命令因环境限制未运行，必须在 PR/阶段报告中明确写出，不能用“静态检查通过”代替运行验收。

## 12. CI 与交付门槛

每个阶段至少有以下 CI Job：

1. `backend-lint-type`：Ruff + Pyright。
2. `backend-unit`：Domain/Application 单元测试。
3. `postgres-integration`：真实 PostgreSQL 18，并断言 `SHOW server_version_num` 为 18.x。
4. `contract-diff`：HTTP、任务、配置和 schema 合同。
5. `frontend-type-unit-build`：TypeScript、Vitest、Vite Build。
6. `compose-smoke`：API、Worker、Coordinator、Redis、PostgreSQL、Nginx。
7. `image-security`：SBOM、依赖漏洞、Secret Scan。
8. `docs-build`：VitePress。

合并规则：

- 一次 PR 只迁移一个明确领域切片或一项平台能力。
- PR 必须写清旧入口、新入口、合同、数据影响、验证和回退。
- 禁止同一 PR 同时迁移框架、改变业务规则和重做 UI。
- `compatibility/` 中每个临时代码必须带迁移清单编号，不允许无所有者永久保留。
- 不提交真实 `.env`、数据库 dump、Token、Cookie、用户数据或外部私有响应。
- 项目级永久测试使用 `verification/`；临时 `tests/` 按仓库规则在提交前删除。

## 13. 提升重构效率的执行规则

1. **先迁移垂直切片，不先做全仓库机械分层。** 每个切片包含 API、Use Case、Repository、Adapter 和验证，完成后才进入下一切片。
2. **先包接口，再替换内部。** 为旧实现建立 Port/Adapter，避免同时重写调用方和被调用方。
3. **只保留一个写路径。** 除经过设计的短期 dual-write 外，禁止旧新实现同时写同一业务对象。
4. **读路径先行。** 先迁移查询和合同，稳定后再迁移有外部副作用的写操作。
5. **OpenAPI 驱动前端。** 后端合同冻结后生成 Client，避免手工维护两套类型。
6. **基线自动化。** 路由、任务、配置、表结构和前端路径全部由脚本生成，不手工复制清单。
7. **迁移即删除。** 一个域完成观察后，及时删除对应兼容路径，避免“双架构永久共存”。
8. **性能以数据决策。** 只有 p95、CPU Profile、DB Query Plan 或队列数据证明有瓶颈时才引入 async、缓存或并发。
9. **外部副作用先幂等。** 任何转存、删除、扣点、订阅、回写在并行化前先建立幂等和对账。
10. **不夹带依赖升级。** 框架、领域、前端和基础设施升级分别提交，失败时能单独回退。
11. **每阶段只盘点一次。** 没有新测试失败或 review 证据时，不重新扫描已经确认的范围。
12. **每阶段压缩为 2-3 个完整批次。** 一个批次完成实现、targeted gate 和 checkpoint 后才进入
    下一批。
13. **每批只运行一次正式 targeted gate。** 开发中的最小复现不冒充批次门禁；高风险批次必须
    同时验证事务、幂等、删除、额度、Ledger、Outbox、claim 和恢复不变量。
14. **完整门禁只在阶段末运行一次。** 统一执行 Ruff、Pyright、pytest、PostgreSQL 18、
    Redis/Celery、Manifest、OpenAPI 和 secret scan；默认 skip 不算通过。
15. **Review 按风险分层。** 高风险事务在阶段内做一次聚焦 review，阶段末再做一次集成 review；
    普通 CRUD 和机械迁移不重复双 review。
16. **每批创建独立本地 checkpoint commit。** 不 amend，不把多个大批次长期留在 dirty
    worktree；push、部署和发布另行授权。
17. **Goal 日志保持紧凑。** 每批只记录 `Stage / Batch`、`HEAD / Commit`、`Implemented`、
    `Targeted gate`、`Stage full gate`、`Pending`、`Blocker` 和 `Next`，详细输出保存在 artifact。

## 14. 关键架构决策记录

| ID | 决策 | 理由 | 代价 |
| --- | --- | --- | --- |
| ADR-001 | 同仓库新主线 | 保留代码历史、行为对照和阶段级回退；减少复制分叉 | 迁移期存在兼容目录 |
| ADR-002 | 模块化单体 | 当前领域高度关联，单库事务有价值 | 必须用依赖规则防止重新耦合 |
| ADR-003 | FastAPI 主入口 + Flask fallback | 可以逐路由替换且保持 URL | 迁移期同时维护 ASGI/WSGI |
| ADR-004 | SQLAlchemy 2 + Alembic | 类型化 Repository 和版本化 schema | 39 表映射工作量较大 |
| ADR-005 | Celery + Redis | 成熟持久任务、重试和进程隔离 | 增加 Redis 和 Worker 运维 |
| ADR-006 | 同步 Domain，选择性 async | 兼容现有 SDK，降低取消和事务复杂度 | 个别高并发路径需单独 async Adapter |
| ADR-007 | 保留 Vue/Naive UI | 避免架构迁移叠加产品重做 | 需要渐进处理现有双 UI 库 |
| ADR-008 | Greenfield 新环境一次启用 | 当前环境无迁移或回退价值；新栈从空状态建立 | 启用前必须阻止其他实例消费相同外部事件 |
| ADR-009 | PostgreSQL 18 空库作为新主线事实来源 | 生产历史数据可清空；避免物理升级并直接使用目标主版本 | 旧业务记录不迁移，切换后必须重新初始化和同步 |
| ADR-010 | 兼容优先，不引入 `/v2` | 现有前端、Bridge 和外部消费者无感迁移 | 旧协议缺陷需保留到独立版本升级 |

## 15. 整体 Definition of Done

最终评审必须逐项确认：

- [ ] 新生产镜像不包含活动 PyArmor 运行依赖。
- [ ] 生产数据库实际运行 PostgreSQL 18，使用新 Volume，旧 PostgreSQL 物理目录未被复用。
- [ ] 所有旧 Blueprint 已迁移，FastAPI 路由清单有所有者。
- [ ] 所有旧任务已迁移，50 个兼容 Key 处理明确。
- [ ] 所有表由 Alembic 管理，启动不再执行散落 DDL。
- [ ] 所有外部服务均通过 typed Client Adapter 调用。
- [ ] API 进程无常驻业务线程和长任务。
- [ ] Worker、Coordinator、Proxy 的生命周期、健康和停止行为明确。
- [ ] 前端 55 个 Vue 文件完成 TypeScript 目标或有经批准的例外清单。
- [ ] 前端不存在未登记的直接 API 字符串和重复 DTO。
- [ ] Contract、Integration、E2E、性能和全环境销毁重建演练全部通过。
- [ ] 登录、配置、任务、媒体入库、Bridge、115、订阅、共享和代理完成真实 Canary。
- [ ] PostgreSQL 18、Redis、`/config`、Compose 和锁定镜像组成的 Greenfield Rebuild Pack 经过实际重建验证。
- [ ] 观察期覆盖完整任务周期，没有未解释的数据或行为差异。
- [ ] 新环境镜像和 entrypoint 不包含旧运行入口或 PyArmor runtime。
- [ ] README、架构文档、运维文档和 AI Context 已更新为新主线事实。

## 16. 实施前仍需落实的环境信息

以下不是架构决策，而是执行 P0/P12 前必须填写的环境参数；不得把真实 Secret 写入本文件：

- 可丢弃的旧版参考数据库 DSN，仅用于合同对照。
- 隔离新版 PostgreSQL 18 数据库 DSN。
- 新 PostgreSQL 18、Redis 和 `/config` 的 Volume 名称/宿主机路径。
- 隔离配置目录和媒体测试目录。
- 旧版、新版 API/Proxy 端口。
- 测试 Emby Library、Bridge 和测试用户。
- 115 测试目录及允许的写操作范围。
- MoviePilot、共享中心、Telegram、HDHive 的测试/Stub 策略。
- 新环境启用窗口、稳定观察期、停止和重建负责人。

## 17. 文档维护规则

- 本文件是重构执行基线；架构、兼容和切换策略发生变化时必须同步更新。
- 每个阶段开始前补充具体 Task Card、文件清单、负责人和验收命令。
- 每个阶段完成后在对应章节记录完成 Commit、验证证据和剩余例外。
- 当前仓库没有 `local/sdlc/` 资产；未来若建立 ADS，应把本文件中的 ADR、领域边界和验证门槛链接进去，而不是复制出互相漂移的版本。
- 文档不得记录明文账号、密码、Token、Cookie、数据库内容或真实私有 URL。
