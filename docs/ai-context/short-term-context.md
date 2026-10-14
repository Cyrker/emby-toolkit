# ETK 短期对话上下文

> 核对日期：2026-09-12。本文是当前原生主线接手入口，不是旧 Flask 项目的启动说明。
> 当前实现以源码、锁文件和实际 Compose 为准；阶段状态见 [goal-tasks.md](./goal-tasks.md)，历史证据见 [goal-log.md](./goal-log.md)。

## 1. 项目与状态

- 当前开发目录由 `git rev-parse --show-toplevel` 确认，目录名为 `emby-toolkit-refactor-mainline`；不要切回旧工作目录执行历史命令。
- 本次核对提交基线为 `main@2e09b04f2ac7`，应用与前端版本均为 `10.8.65`。工作树包含未提交的部署、授权和出站直连修复；Git HEAD 不代表镜像包含的完整源码。
- P0–P8、P10–P12 Local 为 `DONE`，P9 为 `LOCAL_DONE`；完整 Live Acceptance 仍为 `HUMAN_PENDING`。不要重新执行旧交接中的“剩余七个任务”，也不要把容器健康等同于全部业务验收。
- 2026-09-12 后续修复已随 `10.8.65-local.20260912-monitor-logs.1` 部署：演员缺少人物缓存时回退到媒体演员名单，保留头像与聚合角色，分集合并常驻/客串演员；补齐分级、IMDb、关键词、类型中文映射，分集剧照提供 `Primary`。重新处理只对缺少主图的分集开启图片刷新、不替换已有图片，并校验人物/头像数量、分级、IMDb 和主图。115 状态查询在明确 Token 失效时受控续期，容量显示支持原始字节及零值；AI 测试改用原生 HTTP，不再导入旧 `database`。镜像上线不等于这些业务已逐项线上验收；仍以 TMDb ID 识别，不要求填写 TVDB ID，也未扩展插件协议。
- 已移除 ETK 自身 Pro 商业门槛；登录、管理员权限、功能开关及第三方服务授权仍然有效。
- 同日后续日志/文件监控修复已部署：历史页统一 `app.log`，不再列出每任务虚拟文件；合并运行日志及最近 100 个根任务的快照，运行文件 10 MiB 轮转、保留 1 份。Coordinator Leader 启动原生 Watchdog Observer，随监控开关/路径更新，失去 Leader 或停机时关闭，不依赖定时任务总开关，也不在启动时全库补扫。新分集所属剧尚无 TMDb ID 时，识别后补父剧及可定位的所属季，不扩展到兄弟集。新增日志区分图片地址准备、实际传输、Emby 回读，以及 MP 通知接收/跳过/入队。部署时补齐 Coordinator/Celery Worker 的统一日志初始化；现场已确认三个配置目录的监听启动与合并日志读取，等待用户重新整理验证真实事件链。

## 2. 当前运行架构

> 2026-09-12 后续修复已随 `10.8.65-local.20260912-playback-events` 部署：播放事件 Worker 接入原生片头检测、115 租约清理、代理租约释放及 Telegram 文字通知；未启用的通知明确跳过，停止生产已退役虚拟导入事件，历史积压以退役结果收敛。配置同步改用纯 ASGI 中间件，修复断连时 BaseHTTPMiddleware 提前结束流导致 Content-Length 错误。历史日志按完整记录倒序、保留堆栈顺序，展示时间/级别/来源、折叠详情及级别/健康检查过滤。现场 93 条积压播放事件已全部结束；真实 Telegram 发送和完整播放场景仍未逐项验收。

Python 3.12、FastAPI/Starlette/Uvicorn、Pydantic Settings、SQLAlchemy 2、psycopg 3、Alembic、PostgreSQL 18、Celery 与 Redis 7。

入口为 `main.py` → `src/etk/bootstrap/cli.py`。同一镜像分角色运行：

| 角色 | 当前职责 |
|---|---|
| `api` | 管理 API、认证、配置与前端静态资源，容器端口 5257 |
| `worker` | Celery 持久任务与外部副作用；`legacy.serial` 队列要求 concurrency=1，名称不表示旧 Flask Worker |
| `coordinator` | Outbox、调度及后台协调；Compose 的调度开关默认关闭，按环境显式配置 |
| `control-plane` | 经 allowlist、命令账本和 Token 控制的 Docker 操作，容器端口 5260 |
| `proxy` | 独立 ASGI Emby/虚拟库 HTTP 与 WebSocket，容器端口 7758 |
| `gateway` | Nginx 8097 → Proxy；`/api/p115/` 分流到 API |
| `migrate` | 一次性 `alembic upgrade head`，退出码必须为 0 |

构建使用 `Dockerfile.refactor` 与 `docker/compose.refactor.yml`。旧 `web_app.py`、`routes/`、`database/` 和顶层 Processor 不是当前请求/任务入口；镜像不复制 PyArmor 主程序。保留的 `handler/`、配置键和兼容类按真实调用链判断，不因名字含 legacy 就删除。

前端为 Vue 3 + Vite + TypeScript 工具链，使用生成的 OpenAPI Client、TanStack Vue Query、Pinia、Vitest、Playwright E2E。路由在 `frontend/src/router/index.ts`；仍存在兼容 JS/Vue 和 `@ts-nocheck`，不能宣称已全部严格类型化。

## 3. 功能、配置与出站边界

- 保留本地 115 授权、整理、STRM、播放、媒体信息，Emby 媒体管理、追剧、演员订阅、自建合集、图片、用户管理和 Telegram Bot 快捷任务。
- **已退役**：共享资源中心、HDHive、TG UserBot 频道监听及旧云端搜索。历史表、配置和部分底层类用于审计/解码，不表示仍可执行。
- 冻结注册表保留 50 个 task key；活动任务为 46 个。四个退役 P9 key 不再接受新 Job，历史任务按 `feature_retired` 跳过。
- MoviePilot 是当前可选订阅源；Webhook 接入原生任务链。Telegram Bot 通知、Chat ID 绑定、管理员快捷任务保留，网页复用 `/api/tasks/run`。
- **Emby、MoviePilot、115 API 与播放直链直接连接**，不受环境 HTTP 代理影响。TMDb、GitHub、Telegram 等受限服务由各自适配器读取 `network_proxy_enabled` / `network_http_proxy_url`；不是“所有外网都走代理”。
- Emby 服务授权保存长期 AccessToken 到兼容键 `emby_api_key`，并用 `emby_auth_mode=user_token` 标记。原生库查询在通过自身权限校验后，将空 Token/全星号值回退到服务端 Token。
- 手动处理页的 Emby 搜索结果可直接重新处理，不要求伪造待复核记录。选择整部剧处理其现有季集，选择单季只处理该季及其分集，单集不扩展到兄弟项；先保存所选范围元数据与路径关联，再逐项提交非递归、仅元数据的持久刷新，并回读身份、标题及非空简介。季集使用独立 TMDb ID；缺失或不匹配的季集编号明确报错，部分完成不冒充成功。
- 手动重新处理缺失 TMDb ID 的电影/剧集时，按明确片名及年份查询；仅唯一的同名同年匹配自动采用，年份冲突、多候选或未匹配时要求在 Emby 人工确认 ID。普通“同步媒体数据”仍不是整库季集补刮入口。已锁定的名称/简介不强制覆盖，不自动重新编号或重命名文件。
- 实时日志按 `timestamp/level/message` 渲染。历史日志从 PostgreSQL 读取最近 100 个根任务及关联子任务，并兼容旧日志文件；每任务当前最多保留 200 条，不能当作完整 Worker 进程日志。
- 配置由 `src/etk/platform/config/` 管理：bootstrap 使用 INI/显式环境别名；动态配置主要在 PostgreSQL `app_settings.dynamic_app_config`，独立配置仍有各自 setting key。备份 `/config` 不能代替数据库备份。

## 4. Proxy 与 Emby 插件

- 保存 `proxy_enabled=true` 不会创建容器或发布端口，必须部署 `proxy` 和 `gateway`。
- `proxy_runtime.py` 装配真实 PostgreSQL 配置、Catalog、Emby 上游与租约；普通 Emby 请求回源，虚拟 ParentId/详情按权限处理，Views 合并配置选择的原生库与虚拟库。
- 标准 `/api/p115/play/<pickcode>` STRM 已在 Proxy 接通 115 HTTPS 302；识别为 115 但解析失败时返回 503，不静默转码。共享虚拟播放不是当前已验收能力，相关路径明确失败。
- `proxy_allow_transcoding` 关闭时拦截转码请求，开启时允许回源；不得替用户改变设置。
- 独立插件 `ETK MediaInfo Bridge` 负责元数据/图片 Provider、媒体流注入、Item ID 与事件回传，不是 8097 网关或 115 302 的实现。
- 插件安装要求显式容器 label 或 `ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES`，且 Server ID 匹配。当前安装器依赖容器独立 IP，`network_mode: host` 仍有识别缺口，标签不能单独解决。
- 2026-09-12 经用户授权，已向 Emby 4.10.0.40 手工安装验证过的 Bridge v1.3.63，运行接口回读版本为 `1.3.63.0`；容器 label 已应用，ETK Origin 已注册，真实启动日志确认 8 组 Harmony hook 激活。安装未升级 Emby 镜像，挂载保持不变；host 网络安装器缺口未修改，fork 也不会自动改变上游下载地址。
- 插件刚安装时 4 个库尚未配置 ETK Provider；后续用户已设置电视剧、动漫库，实际回填验证使用动漫库。其他库以实时配置为准，不因插件加载成功推断全部 Provider 已启用；不擅自覆盖刮削器、NFO 或实时监控策略。

## 5. 临时开发部署与后续正式发布

- 目标主机使用 SSH 别名 `debian-lan`，不是旧文档中的 `unraid`；真实连接值不复制到项目文档。
- 使用 Base + `docker/compose.debian-lan.yml`，固定 project `etk-refactor-greenfield`；命令显式带 `--env-file` 和绝对 Compose 路径。
- Base 默认宿主机端口为 API 15257、Gateway 18097；Debian 环境设置 `ETK_API_PORT=5257`、`ETK_PROXY_PORT=8097`。`EXPOSE` 和 UI 端口不能替代 Docker 发布映射。
- `/config` 和四个媒体盘映射以 overlay 为准；PostgreSQL、Redis、Control Plane runtime 保留原 named volumes，不改挂旧数据库目录。
- **临时开发验证**：镜像在 Mac 构建 `linux/amd64`，导出并核对传输前后 SHA-256，再传 Debian 加载运行。这是当前调试阶段的临时方式，不是长期交付规范。
- **正式交付方向**：功能正常、版本稳定后发布到 DockerHub，由 Debian 拉取发布镜像部署，不再要求手工传镜像文件。本次文档更正不代表现在执行 DockerHub 发布。
- 升级前保留 Compose、环境文件、配置、数据库及旧镜像回滚点，应用按角色更新。不把旧单容器 `docker cp` 当默认流程；不动 DB/Redis，不执行 `prune`、`down -v` 或删除旧归档。
- 备份保留规则（2026-09-12 用户确认）：日常部署仅保留上一份可回滚备份及上一版镜像；新版本验证成功后定向清理更早备份、无容器引用的旧开发镜像和已导入的临时镜像包。初始旧项目归档包含独立旧数据库，用户明确要求另外保留；不清理其他服务的备份。
- 2026-09-12 最新已部署 `10.8.65-local.20260912-playback-events`，Mac 构建 amd64 并核对传输摘要，六个应用角色 healthy；数据库迁移版本不变，DB/Redis 容器及全部媒体挂载保持不变。文件监听启动、格式化历史日志读取及页面/静态资源通过；启动后检查未见处理器缺失或响应长度错误。部署前 93 条 pending 播放事件已确认结束，未启用的通知/检测按配置跳过，退役事件明确跳过。普通回滚点仅保留上一份 monitor-logs.1，初始归档另存；未主动扫描、补刮或构造真实播放事件，完整 Live 仍待验收。

## 6. 定位与验证

问题域读 `src/etk/domains/`，HTTP 注册看 `src/etk/bootstrap/app_factory.py`，任务执行看 `src/etk/processes/worker.py`，契约看 `verification/contracts/`。详细映射见 [长期知识库](./long-term-knowledge-base.md)。

在仓库根目录、Python 3.12 环境中使用现有依赖：

```bash
uv sync --frozen
uv run python main.py --help
uv run pytest -q verification/application/p9/test_proxy_runtime.py
uv run python scripts/refactor/check_manifests.py
npm --prefix frontend run typecheck
npm --prefix frontend run check:api-boundary
npm --prefix frontend run test:unit
npm --prefix frontend run build
```

按变更选择测试，不默认全跑。E2E 为 `npm --prefix frontend run test:e2e`，目录为 `frontend/e2e/`；后端永久验证在 `verification/`，不是 `tests/` 或不存在的 `verification/e2e/api`。数据库/Greenfield 验证必须使用独立测试库与明确归属的可丢弃环境，不能对生产运行销毁重建脚本。

旧方案和三份 handoff 是有日期的历史合同；旧 HEAD、dirty 列表、待办和操作边界不替代当前任务授权。改文档不授权部署、发版、安装插件或清理数据。
