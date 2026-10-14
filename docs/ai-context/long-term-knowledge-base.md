# ETK 长期知识库

> 核对日期：2026-09-12。描述当前原生主线，不再把重构前 Flask/PyArmor 架构作为运行指南。
> 优先级：当前用户授权与适用 AGENTS.md → 当前源码/锁文件/实际部署 → 本文。阶段状态见 [goal-tasks.md](./goal-tasks.md)，历史证据见 [goal-log.md](./goal-log.md)。

## 1. 项目定位与活动范围

Emby ToolKit 是媒体管理与入库增强工具，不是 Emby 服务端替代品。当前保留 115 授权、整理、上传、STRM、媒体信息与播放；元数据、图片、演员、追剧、订阅、合集、清理、用户管理和 Telegram Bot 快捷任务。

2026-09-10 移除了 ETK 自身 Pro 商业门槛，没有移除登录、管理员权限、功能开关或第三方授权。

2026-09-11 已退役共享资源中心、HDHive、TG UserBot 频道监听及相应云端搜索。MoviePilot 是当前可选订阅源；Telegram Bot 通知、Chat ID 绑定、管理员快捷任务保留。历史共享表、配置键、迁移和底层类保留不等于功能仍开放，不能根据旧 P9 合同重新启用它们。

## 2. 当前技术栈

| 层 | 实际技术/入口 |
|---|---|
| Python | 3.12，约束 `>=3.12,<3.13`，`pyproject.toml` + `uv.lock` |
| HTTP | FastAPI、Starlette、Uvicorn；`main.py` → `src/etk/bootstrap/cli.py` |
| 配置/装配 | Pydantic Settings、AppContainer、`src/etk/platform/config/` |
| 数据 | PostgreSQL 18、SQLAlchemy 2、psycopg 3 / psycopg_pool、Alembic |
| 后台 | Celery、Redis 7、PostgreSQL Job/operation/Outbox、独立 Coordinator |
| 代理 | ASGI Proxy + Nginx Gateway，HTTP/WebSocket 直连 Emby |
| 前端 | Vue 3、Vite、TypeScript 工具链、Naive UI、Pinia、TanStack Vue Query、openapi-fetch |
| 验证 | pytest、Ruff、Pyright、Vitest、Playwright E2E、契约与架构检查 |
| 构建 | `Dockerfile.refactor`；Node 20 构建前端，Python 3.12 运行镜像 |
| 文档 | 根 package 的 VitePress，不等于 frontend package |

应用版本从 `constants.py:APP_VERSION` 读取，前端版本在 `frontend/package.json`；发版时核对两者，不以文档快照判断升级。

Flask、gevent、psycopg2、requests 在 pyproject 的 dev 组中用于旧基线验证，不是原生运行宿主。`handler/` 与部分兼容工具仍打包，具体活动依赖以调用链与镜像检查为准，不能理解成“仓库已删除全部旧代码”。

## 3. 运行拓扑与职责

```text
管理端 → api:5257 → Use Case / Repository → PostgreSQL 18
                                      → Job / Outbox → Coordinator → Redis → Worker

Emby 客户端 → gateway:8097 → proxy:7758 → 真实 Emby
                          └ /api/p115/ → api:5257

Worker → control-plane:5260 → 被 allowlist 授权的 Docker 操作
```

- API 提供认证、HTTP 路由、配置和静态资源，不使用旧 `web_app.py` 启动。
- Worker 执行持久任务与副作用；当前 CLI 强制 `legacy.serial` concurrency=1、solo pool。这是队列契约，不是未迁移 Flask 的证据。
- Coordinator 协调 Outbox、计划任务和后台工作。Compose 默认 `ETK_COORDINATOR_SCHEDULES_ENABLED=false`，容器运行不表示所有调度已启用。
- Control Plane 通过 Token、命令账本和 allowlist 控制 Docker 操作；普通服务重启权限与 Emby 插件安装名单不同。
- Proxy 使用 PostgreSQL 配置、Catalog、用户并发限制和播放租约，转发 Emby HTTP/WebSocket。
- Gateway 使用 `docker/nginx.refactor.conf`，不是管理前端的替代入口。
- migrate 一次性执行 `alembic upgrade head`；必须成功退出。迁移可能改变 schema，恢复旧镜像不代表数据库自动回退。

Base Compose 共九个 service：上述七个加 db、redis。不是旧版“一个 ETK 容器 + 一个数据库”。

## 4. 当前代码地图

以下路径均相对当前仓库根目录。

| 对象 | 优先入口 |
|---|---|
| 进程命令 | `main.py`、`src/etk/bootstrap/cli.py` |
| API 注册/装配 | `src/etk/bootstrap/app_factory.py`、`src/etk/platform/container.py` |
| 设置 | `src/etk/platform/config/`、`src/etk/domains/system/configuration.py` |
| 登录、授权、用户 | `src/etk/domains/identity/` |
| 媒体、图片、Bridge、事件 | `src/etk/domains/media/` |
| 115 | `src/etk/domains/p115/` |
| 入库 | `src/etk/domains/ingestion/`、`src/etk/processes/p6_ingestion.py` |
| 订阅/追剧/洗版/MoviePilot | `src/etk/domains/subscription/` |
| 合集/封面 | `src/etk/domains/collection/` |
| 人物后台工作 | `src/etk/domains/people/` |
| Job 与任务控制 | `src/etk/domains/operations/`、`src/etk/platform/jobs/`、`src/etk/platform/tasks/` |
| Worker/Coordinator/Control Plane | `src/etk/processes/worker.py`、`coordinator.py`、`control_plane.py` |
| Proxy | `src/etk/processes/proxy.py`、`proxy_runtime.py` |
| schema | `alembic.ini`、`migrations/`、`src/etk/platform/db/` |
| 前端 | `frontend/src/router/index.ts`、`frontend/src/api/`、`frontend/src/stores/`、`frontend/src/components/` |
| 契约/归属/冻结基线 | `verification/contracts/`、`scripts/refactor/` |

顶层 `core_processor.py`、`watchlist_processor.py`、`task_manager.py`、`scheduler_manager.py`、`reverse_proxy.py`、`routes/`、`database/` 等是旧实现/验证参考，不是新功能的默认修改入口。新 Dockerfile 不复制旧 routes/database/tasks 目录及 PyArmor 主程序。遇到保留的 Legacy 类或旧配置键，先查原生装配的实际使用，不按命名清理。

## 5. 配置、身份与网络

### 配置持久化

- `APP_DATA_DIR` / `CONFIG_DIR` 指定运行配置目录，容器使用 `/config`。
- `LegacyConfigAdapter` 不导入旧全局配置模块，保持 INI/bootstrap 与数据库动态键兼容。
- bootstrap 先取默认/INI，再应用显式支持的环境别名；动态配置读 `app_settings.dynamic_app_config`，独立 setting 由各领域维护。
- `Settings` 不直接使用 Pydantic 的环境或 dotenv source；不能假设任意同名环境变量可覆盖数据库设置。DB URL、Redis、角色参数还有各自 runtime 入口。
- 配置更新采用校验和合并，不能用前端脱敏响应覆盖真实秘密。API 与 Proxy 会同步动态配置。
- 115 凭据、上传监控、播放状态不全在 dynamic_app_config；备份配置目录不能替代 PostgreSQL 备份。

### Emby 身份

服务授权使用管理员身份换取长期 Token，字段名为 `emby_api_key`，`emby_auth_mode=user_token` 表示服务授权完整；用户名/密码不作为长期服务凭据保存。授权成功、插件安装成功、媒体库 Provider 启用是三个不同结果。

配置响应中的 Token 为 `********`。用户 Views 路由在 `secondary_router.py` 中先验证 ETK 自身权限，再将空/全星号 Token 回退到服务端 Token；不无条件覆盖显式客户端 Token。

### 出站策略

Emby、MoviePilot、115 API/播放直接连接，不使用环境代理。TMDb、GitHub、Telegram 等受限服务由各自适配器按 `network_proxy_enabled` / `network_http_proxy_url` 使用代理；不能简化成“只有局域网直连”或“纯外网全部代理”。

检查具体调用时看原生 adapters、`handler/emby.py`、`handler/moviepilot.py`、115 direct-client 与 `proxy_runtime.py`。定向验证在 `verification/architecture/p11/test_legacy_lan_direct_http.py`、`test_p115_direct_http.py`。不要为一个 Emby 请求关闭 TMDb 的代理配置。

## 6. 媒体与 Bridge 的保留合同

以下描述后端与独立插件的集成合同，**不等于完整业务验收**；当前安装状态见下方“插件安装、更新与兼容”。

- `p115_mediainfo_cache.mediainfo_json` 为规范化媒体流来源，`media_metadata` 为元数据来源；新入库不生成或依赖 NFO、媒体信息 JSON 侧车。
- 整理准备媒体身份、元数据和资产路径后通知 Emby 扫描。插件回传 Item ID 再继续后续处理；按原生 media/p115/ingestion 域及 p6 进程定位。
- Bridge 的 Movie/Series/Season/Episode Provider 依据 STRM pick code/SHA1 或实体路径恢复元数据；Season 与 Episode 索引分别处理，不能用集号覆盖季号。
- 旧媒体的普通同步会合并 Emby 路径到 `asset_details_json`，保留已有资产记录；缺少路径关联时增量任务也会重新同步。手动重新处理按电影/整剧/单季/单集限定范围，先抓取并保存所选季集，再逐项持久刷新与回读。季集分别保存自己的 TMDb ID、父剧 ID、季号和集号，不把整部剧 ID 写作分集 ID；季详情在一次任务内复用，缺失编号或已有身份冲突要求人工核对，不猜测映射。
- 缺少根 TMDb ID 时，`reprocess_metadata.py` 按明确片名和年份查询，只有唯一同名同年结果才自动识别；歧义、年份冲突和缺少年份时停止并提示人工确认。此能力接在手动重新处理，不表示普通同步会自动完整刮削全库。名称/简介锁定时不强制改写，不自动重命名文件或调整季集编号。
- 逐项刷新使用非递归、仅元数据模式，不请求替换现有图片。任务区分选中、保存、回填确认和失败数量；只有 Emby 身份、标题、非空简介回读一致才计入确认。部分完成保持部分结果，不因父剧成功就宣称整剧补刮成功；插件 `refresh.request` 自动事件处理仍是独立缺口。
- 2026-09-12 后续修复已随 `10.8.65-local.20260912-monitor-logs.1` 部署，扩展上述规则：演员名单保留头像地址，人物表缺失不丢掉演员，有人物缓存时优先使用缓存姓名；分集合并该季常驻演员与客串。分集剧照同时提供 `Primary`，只对缺主图的分集执行非替换图片刷新；原有主图和元数据锁定条目保留。回读增加演员/导演及其可用头像数量、IMDb、分级和分集主图检查；不把镜像上线当作已完成线上回填。
- 实体媒体流仍由 Emby 原生探测；STRM 流通过 `POST /Items/{Id}/ETKMediaInfo` 注入。刷新恢复与 Item ID 重绑不等同于重跑完整整理链。
- 图片策略读取 Emby `TypeOptions.ImageOptions`；`media_image_policy_cache` 记录按项策略，`media_image_cache` 内容仓库仅保存实际引用图片。普通同步保留手动选图，“替换现有图像”才强制重选。
- 四个源图片路径不等于仓库文件清单。Episode 正式剧照缺失时用 `screenshot_hash` 保存兜底截图，正式图片接管后清理无引用缓存。
- 演员角色持久化不保存“饰 / 配”前缀，响应按配置添加。关键词/工作室中文化遵循映射，Emby `OfficialRating` 使用美国分级映射。
- TMDb 合集使用媒体库 `ImportCollections`、最小数量规则和 `collections_info` 激活状态。明确 404 才进入匹配类型/ID 的清理，不能把图片请求失败当删除依据。
- Emby 事件通过 Bridge 的 `/api/emby/events` 回传；MoviePilot Webhook 有独立原生入口。播放、手动编辑、图片与删除是不同合同，普通刷新不能当用户编辑。
- 主动删除使用 prepare/commit 与持久状态；115 删除还需联动开关、身份、取消及副作用确认。本文不构成删除授权。
- 共享资源中心退役后，旧共享片头上传、中心登记/转存等外部写入不能因旧文档而恢复。本地媒体信息/片头按当前 native owner 执行。

### 插件安装、更新与兼容

名称 `ETK MediaInfo Bridge`，DLL `ETKMediaInfoBridge.dll`，计划任务 Key `ETKMediaInfoBridgeUpdate`。源码是独立仓库，本项目保留安装器、下载代理和 Bridge API。

- 安装器 `handler/etk_plugin_update.py` 经 Control Plane 执行，要求容器 label `com.embytoolkit.control-plane.plugin-install=true` 或 `ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES`，并核对已授权 Server ID。
- 当前 URL 枚举依赖 Docker 容器独立 IP，host 网络存在识别缺口。编排已写 label 不表示运行容器已应用，更不表示插件已安装。
- 原生下载代理在 `src/etk/domains/media/bridge_client.py`，仍指向上游；fork 不会自动切换下载源。
- 插件抓取器名称为 `ETK Metadata`、`ETK Images`，安装后需按媒体库确认启用。
- 上游 v1.3.63 声明支持 4.9.x。本轮对话以真实 4.10.0.40 程序集完成离线构建、153 个类型加载、8,073 次 IL 引用解析及 8 组 Harmony 挂接检查，未发现该层兼容问题；构建有 6 组依赖冲突警告，没有生产业务/其他插件共存验收。
- 2026-09-12 后续经用户授权，已手工安装验证过的上游 DLL；目标 Emby 回读 `1.3.63.0`，更新任务已注册，ETK Origin 配置成功，真实启动日志确认 8 组 Harmony hook 激活。通过一次原镜像容器重建应用插件与 label，镜像 ID、host 网络和全部挂载保持不变；未修复自动安装器的 host 网络识别缺口。
- 刚安装后的只读核查曾发现 4 个媒体库没有配置 ETK Provider；后续用户已配置电视剧、动漫库，真实回填验证使用动漫库。其他库以实时配置为准。不得为了启用插件而重新授权并调用 `_apply_library_defaults`，该流程还会更改实时监控、NFO 等策略，需另行确认范围。
- 插件加载与 hook 启动已有真实服务证据；深层元数据、图片、事件、删除及其他插件共存的完整业务验收仍未完成。8097 和标准 115 302 可独立于 Bridge 工作。

## 7. 任务、前端与 Proxy

2026-09-12 最新补充已部署：`processes/filesystem_monitor.py` 将真实 Watchdog 事件接入既有持久入库链；Observer 只由 Coordinator Leader 启停，跟随配置更新，不依赖 cron 总开关，不在启动时扫描全部旧文件。已有漏项仍需查漏或定向处理。新分集识别出原先无 TMDb ID 的父剧时，同时准备必要父级元数据，不处理兄弟集。现场确认监听启动，完整新增文件刮削仍待用户实际整理验证。

同批日志修复将历史页整合为 `app.log`（兼容旧任务日志链接但不再逐任务列文件）。运行日志通过跨进程文件锁合并，10 MiB 轮转、保留一份归档；任务部分仍为最近 100 个根任务的有限快照。Coordinator CLI 和 Celery `setup_logging` 信号均接入统一日志初始化，避免只在 API 留下日志。图片源地址就绪、真正下载及 Emby 回读是不同阶段；MoviePilot 通知的接收、忽略原因和持久入队均记录，但不能凭“没有持久记录”断言 MP 从未发送过请求。现场还发现部分 `emby.event.user.*` 播放副作用缺少 Worker 处理器；本次记录为独立缺口，不等同于 MP 通知或文件监听失败。

Job/operation/Outbox 以 PostgreSQL 为持久事实源，Redis 为任务消息基础设施。外部写入前需要 checkpoint；未知结果需要 authoritative readback，不能凭超时重复发送或把取消视作无副作用。

后续修复已随 `10.8.65-local.20260912-playback-events` 部署：Worker 注册用户事件执行器，使用原生片头检测、115 播放清理、持久代理租约释放及 Telegram 文字通知，不导入旧 routes/database/reverse_proxy。通知遵循 playback 开关、Bot 和接收目标配置；部分发送后结果不明不自动重放，暂时失败设重试上限。已退役虚拟导入停止生产新事件，既有事件以 feature_retired 结束。API 配置同步使用透传式 ASGI 中间件，避免 BaseHTTPMiddleware 在断连时将未传完的流误标为完整结束；不删除 Content-Length 或 Content-Range。历史日志按完整记录而非逐行倒序，堆栈保持原序并折叠；成功健康检查/状态轮询只在视图默认隐藏，不删除原始日志。现场积压播放事件已收敛，不等于全部外部功能逐项验收。

实时日志是结构化对象，前端不能直接 `join()`。历史页面经 `PersistentJobLogStorage` 使用现有日志 API 读取最近 100 个根任务及其明确关联的子任务，展示每任务当前保留的日志、最终状态与错误摘要；旧 `app.log*` 文件仍兼容。当前日志快照最多 200 条/任务，不是完整进程日志，也不能恢复已截断内容。

`src/etk/platform/tasks/catalog.py` 保留 50 个冻结 key，四个 P9 key 已退役，活动列表 46 个。新提交在 Job 创建前被拒绝，历史任务按 `SKIPPED / feature_retired` 跳过，任务链过滤退役 key。清单与实际接线共同证明状态。

前端路由以 `frontend/src/router/index.ts` 为准，`/shared-resources` 已移除；不维护第二份完整 URL 清单。API 使用生成客户端和现有 transport，Pinia 管 auth/UI、Vue Query 管服务端状态；兼容模块仍有 `@ts-nocheck`，最终权限以服务端为准。

任务中心“网页与 Telegram 快捷任务”复用 `/api/tasks/run` 及现有菜单；空配置使用六项默认值。`system-auto-update` 按当前原生策略可能跳过，不等于自动 DockerHub 更新。细节见 [快捷任务说明](../zh/guide/quick-tasks.md)。

Proxy 装配在 `proxy_runtime.py`，普通 HTTP/图片/Range/WebSocket 回源 Emby，虚拟 ParentId/详情与 Views 由 Catalog 按权限处理。标准 115 STRM 解析直链返回 302；已识别 115 但解析失败返回 503，不静默转码。共享虚拟播放路径仍明确失败，不能把保留协议误写成完整支持。转码是否回源由 `proxy_allow_transcoding` 决定。

## 8. 构建、部署和回滚

### 编排与端口

- 采用 `Dockerfile.refactor` + `docker/compose.refactor.yml`；`docker/compose.debian-lan.yml` 只是 overlay，不能单独运行。
- 前端产物位于镜像 `/app/static`，镜像包含 Nginx、ffmpeg 和原生 Python 入口。
- Base 宿主机 API 默认 15257、Gateway 默认 18097，容器内为 5257、8097。Debian 配置为 `ETK_API_PORT=5257`、`ETK_PROXY_PORT=8097`。
- Compose 首文件在 docker 子目录时不能依赖工作目录猜测 .env；显式使用 `--env-file`、project 与完整文件路径。校验用 `config --quiet` 或结构化提取，避免输出秘密。
- UI 的 proxy_enabled/proxy_port 不会创建 Docker 服务或改端口映射，Proxy/Gateway 必须实际存在。

### 数据与权限

Debian 的配置和四个媒体盘 bind mounts 以 overlay 为唯一映射清单，本文不复制私有盘路径。API、Worker、Coordinator、Proxy 有媒体映射；Control Plane 使用配置、runtime Token 和 Docker Socket。

PostgreSQL 18 使用 `pg18_data:/var/lib/postgresql`，Redis 使用 `redis7_data:/data`，控制面凭据使用 `control_plane_runtime`。固定 project `etk-refactor-greenfield`，更名会改变默认 volume 名。不能把当前库改挂旧 PostgreSQL 目录。

named 配置卷切换 bind mount 前需比对/迁移已有内容并保留旧卷。旧 P12“从空库重建”只适用于明确可丢弃的测试环境，不适用于已经接收真实数据的 Debian 服务。

Docker Socket 的 `:ro` 挂载不等于 Docker API 只读权限；边界在 Control Plane 的认证、allowlist 与动作校验。未设置 Watchtower enable label 也不保证禁更，需看 Watchtower 是否启用 label-only；当前本地镜像流程不依赖 Watchtower。

### 临时开发操作与正式交付方向

目标使用 SSH 别名 `debian-lan`。**当前调试阶段临时采用** Mac 构建 linux/amd64、导出并核对传输前后 SHA-256，再传 Debian docker load 的方式，不在 Debian 构建；这不是长期交付规范。

**功能正常、版本稳定后的正式交付方式是发布到 DockerHub，由 Debian 拉取发布镜像部署**，不再要求手工传镜像文件。当前本地镜像版本不能当作已经发布的 DockerHub 版本；具体发布仍在后续发版任务中执行。

获准升级时：

1. 本地验证变更及目标架构镜像，核对 Compose 端口、挂载、project、镜像。
2. 备份编排、环境文件、配置、PostgreSQL，保留旧镜像并记录 DB/Redis 容器身份。
3. 临时开发时导入镜像，正式交付时从 DockerHub 拉取已发布镜像；migrate 必须 exit 0，按 `--no-deps --no-build` 更新获授权的应用角色，不无关重建 DB/Redis。
4. API/Proxy 就绪后启动 Gateway，验证登录、媒体库、播放等业务，不只看 health。
5. 核对数据卷未变化；失败按备份与 schema 兼容性回退，不执行 prune、down -v 或删除旧归档。

日常部署只保留上一份可回滚备份及上一版镜像；新版本验证成功后，定向清理更早的部署备份、无容器引用的旧开发镜像和已导入的临时镜像包，不使用全局 prune。初始旧项目归档包含独立旧数据库，2026-09-12 用户明确要求单独保留；其他服务备份不在此规则的清理范围内。

旧 unraid + 单容器 docker cp 流程只适用于被明确点名的旧实例，不是当前更新入口。静态资源错误仍需检查权限与路由，但不能为排查自动覆盖生产 /app。

## 9. 验证入口

仓库根目录使用 Python 3.12 和锁文件，首次准备依赖为 `uv sync --frozen`。前端按 Dockerfile 使用 `npm ci --legacy-peer-deps`。普通文档修改不需要重新安装依赖。

```bash
uv run python main.py --help
uv run ruff check <本次修改的Python路径>
uv run pyright <受影响路径>
uv run pytest -q <verification下的定向测试路径>
uv run python scripts/refactor/check_manifests.py
npm --prefix frontend run typecheck
npm --prefix frontend run check:api-boundary
npm --prefix frontend run test:unit
npm --prefix frontend run build
npm --prefix frontend run test:e2e
npm run docs:build
```

- 后端分层验证在 `verification/unit/`、`application/`、`architecture/`、`integration/`，永久测试不放临时 tests。
- 前端 E2E 为 `frontend/e2e/`，旧计划中的 verification/e2e/api 不存在。
- DB/Redis 集成测试必须选择隔离连接和对应测试开关，不能带入生产连接。`scripts/refactor/run_p12_greenfield.py` 是可丢弃环境工具，不是线上更新脚本。
- 文档按链接、源码路径、命令与 VitePress 构建验证，不为改文档重跑全部业务测试。
- 两个 refactor workflow 接受 PR/手动触发，push 分支过滤仍为 codex/refactor-mainline；提交 main 不等于这两个门禁自动运行。发版前单独核对，不在文档任务中改 CI。

## 10. 文档与证据维护

| 文档 | 当前性质 |
|---|---|
| short-term-context.md | 快速接手、当前入口和范围 |
| long-term-knowledge-base.md | 当前实现地图、保留合同和维护边界 |
| goal-tasks.md | 阶段状态事实源；Local 与 Live 分开 |
| goal-log.md | 按日期保留证据，旧失败/通过不代表当前状态 |
| full-project-refactoring-execution-plan.md | 重构前基线、设计与门禁，不是现役部署手册 |
| 三份 P7/P11/P12 handoff/remaining plan | 历史执行起点，不重做旧待办或恢复已退役功能 |

文档必须区分“源码有实现”“本地测试通过”“镜像已部署”“业务验收通过”。8097 有限 canary 不代表所有 Bridge、115 外部写入、故障恢复及长期观察完成；LIVE 仍为 HUMAN_PENDING。

不要复制真实凭据、内网 URL、私有媒体路径、数据库或完整环境文件到公开文档。运维值使用别名与私密配置。历史日志不抹掉，不因状态变化批量勾选旧方案检查表。
