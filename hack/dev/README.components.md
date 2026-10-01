# 启动开发组件

先配置数据库、消息队列和加密密钥，安装前端依赖 `cd frontend/app && pnpm install --frozen-lockfile`
启动命令读取仓库根目录的 `.env`（如果存在）及进程环境，不生成配置、不迁移数据库

本地 dbx 的 PostgreSQL 监听 `5432`，需在 `.env` 中设置指向 `hatchet` 库的 `DATABASE_URL`；未配置时 Hatchet 默认使用 `5431`
Hatchet 要求该库的默认时区为 `UTC`，可通过 `ALTER DATABASE hatchet SET timezone TO 'UTC';` 设置
首次初始化登录账号使用 `task seed-dev`，账号由 `ADMIN_EMAIL` 和 `ADMIN_PASSWORD` 配置

```bash
task all         # 并行启动下列四个组件，日志带组件前缀
task api         # HTTP API，默认 8080
task controller  # controllers：Task、OLAP、Retention 等控制器
task engine      # scheduler grpc-api：调度器与 gRPC API，默认 7070
task web         # Vite，默认 5173，/api 请求代理至 API
```

controller 和 engine 都使用 `cmd/hatchet-engine`，启动脚本在加载 `.env` 后固定各自的 `SERVER_SERVICES`
Go 组件使用 `go run`，修改代码后重新运行对应命令
engine 使用仓库维护的默认语义版本，保证 SDK 正确判断服务端能力
Web 缺少文档索引时会通过现有生成器生成该资源，需要 Python 3.10+；生成内容位于 Git 忽略的目录
`task all` 中组件启动失败会终止其余组件，Ctrl+C 会停止组件并清理子进程

| 配置 | 默认值 |
|---|---|
| `CONTROLLER_HEALTHCHECK_PORT` | `8734` |
| `CONTROLLER_PROMETHEUS_ADDRESS` | `:9091` |
| `ENGINE_HEALTHCHECK_PORT` | `SERVER_HEALTHCHECK_PORT`，未设置时 `8733` |
| `ENGINE_PROMETHEUS_ADDRESS` | `SERVER_PROMETHEUS_ADDRESS`，未设置时 `:9090` |
| `WEB_PORT` | `5173` |

指标服务仍由 `SERVER_PROMETHEUS_ENABLED` 控制
若本地 Prometheus 已占用 9090，可设置 `ENGINE_PROMETHEUS_ADDRESS=:9092`
非默认 API 地址通过 `VITE_API_PROXY_TARGET` 配置 Web 代理目标
Web 通过现有 Vite 代理访问 API，无需启动 Caddy
CI 运行这些命令时必须设置 `SERVER_SECURITY_CHECK_ENABLED=false`
已有 `start-*` 命令保持原有行为
