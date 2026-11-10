# embytool

独立维护的 Emby 媒体管理工具，提供 115 网盘整理、STRM 入库、元数据补全、追剧订阅、媒体管理与播放代理。

- [用户文档](docs/zh/index.md)
- [源码](https://github.com/Cyrker/emby-toolkit)
- [问题反馈](https://github.com/Cyrker/emby-toolkit/issues)

## 当前主线

后端使用 Python 3.12、FastAPI、PostgreSQL 18、Celery 和 Redis；前端使用 Vue 3、TypeScript 与 Naive UI。

原生入口为 `main.py` 和 `src/etk/`；应用按 API、Worker、Coordinator、Control Plane、Proxy 与 Gateway 分角色运行。构建使用 `Dockerfile.refactor`，编排使用 `docker/compose.refactor.yml`；不要使用仓库中保留的旧版单容器入口部署本主线。

本项目与原项目的版本、维护和发布渠道独立。尚未发布的镜像或插件版本，不应使用上游发布包冒充本项目产物。

## 兼容性

为接续现有安装，保留内部 `etk` Python 包名、历史配置键、数据库结构、任务标识、Emby 插件 GUID、程序集名及 Provider 协议。`embytool` 与原有 `etk` CLI 入口并存。这些是兼容标识，不代表继续使用上游发布渠道。

Bridge 插件由独立仓库维护；保留 `ETKMediaInfoBridge.dll` 资产名，支持现有插件的原位升级。不要让多个实例同时整理同一批媒体，也不要同时安装两个身份相同的 Bridge 插件。

## 本地验证

```bash
uv run python main.py --help
npm --prefix frontend run typecheck
npm --prefix frontend run test:unit
npm --prefix frontend run build
npm run docs:build
```

数据库集成验证必须使用独立测试库。构建成功、容器健康与真实媒体事件验收是不同的验证层级。

## 许可证与来源

本项目是基于 [Emby ToolKit](https://github.com/hbq0405/emby-toolkit) 的修改版，自 2026-10-18 起以 **embytool** 品牌独立维护；不代表原作者或原项目。

保留原项目及贡献者的版权和来源说明，沿用 [GNU AGPL v3](LICENSE)。修改、分发及通过网络向用户提供服务时，应遵守对应源码和许可义务。新品牌、图标与独立维护不改变继承代码的许可证。
