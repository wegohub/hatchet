# 本机 Docker 部署

Hatchet 使用官方 v0.110.5 镜像和 PostgreSQL MQ。MinIO 独立保存对象，不与任务数据库共用卷。模板绑定本机回环地址：

| 服务 | 地址 |
|---|---|
| Hatchet API / 控制台 | http://localhost:8080 |
| Hatchet gRPC | localhost:7077 |
| MinIO S3 API | http://127.0.0.1:9000 |
| MinIO 控制台 | http://127.0.0.1:9001 |

已有私有 `.env` 时，从仓库根目录执行：

```sh
python3 sdks/wego/scripts/setup-minio.py
docker compose -f sdks/wego/deployment/compose/docker-compose.yml up -d --build --wait minio
```

新部署先复制 `.env.example` 为 `.env`，填写 PostgreSQL 的 DATABASE_URL，再执行以上配置步骤。脚本仅补齐缺失/占位的 MinIO 凭证和 codec 密钥，保留已有配置，文件权限为 0600。凭证不提交；端口可通过 MINIO_API_PORT / MINIO_CONSOLE_PORT 调整。控制台登录使用私有 `.env` 中的 MINIO_ROOT_USER / MINIO_ROOT_PASSWORD。

MinIO 的最新社区发布 [RELEASE.2025-10-15T17-29-55Z](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z) 要求从官方源码构建容器。本模板固定该发布、源提交与构建基础镜像 digest，生成本地镜像 `wego-minio:RELEASE.2025-10-15T17-29-55Z`；它不是 Hatchet 或 MinIO 的第三方修改镜像。构建需要访问 Go 模块代理。

MinIO 使用 `minio_data` 卷和健康检查。不要通过 `down -v` 清理仍需恢复的流：删除卷会同时删除历史引用对象。服务重建会保留数据卷。

运行 [真实 codec 示例与测试](../../examples/codec/README.md)。示例通过 AWS S3 SDK 检查或创建专用 `wego-codec` 桶；创建权限仅用于示例初始化。实际应用可预建桶，使用仅允许所需目录 PUT/GET 的凭证。

## 开启租户 Durable Streams

从仓库根目录运行以下脚本，然后粘贴租户 token；输入内容不会显示：

```sh
python3 sdks/wego/scripts/enable-durable-streams.py
```

脚本先访问 Hatchet API 验证 token，再事务开启该租户的 `durable_streams`。仅修改这一项授权，不影响其他租户及其他 entitlement；重复执行不会改变已经开启记录的更新时间。数据库必须已经完成 Hatchet 迁移，连接账号需要读 `Tenant`、读写 `tenant_entitlement` 的权限。该工具用于自托管部署，SDK 运行时不直接操作数据库。

默认 API 为 `http://localhost:8080`，端口读取 `.env` 的 `HATCHET_HTTP_PORT`；可使用 `--api-url` 或 `HATCHET_CLIENT_SERVER_URL` 覆盖。数据库 URI 优先读取环境中的 `DATABASE_URL`，其次读取此目录的私有 `.env`；可用 `--env-file` 指定其他配置文件。

有本机 `psql` 时直接连接；没有时使用 PostgreSQL 容器内的 `psql`，默认容器 `dbx-postgres`，可通过 `--db-container` 或 `WEGO_POSTGRES_CONTAINER` 指定。地址为 `host.docker.internal` 时自动使用容器。连接使用 URI 中的数据库账号、TLS 和参数；密码不会进入命令行。

也支持已有 token 文件或环境变量 `HATCHET_CLIENT_TOKEN`，以及 `--token-stdin` 的标准输入模式。例如：

```sh
python3 sdks/wego/scripts/enable-durable-streams.py --token-file /path/to/token.env
python3 sdks/wego/scripts/enable-durable-streams.py --db-container postgres-container --api-url http://localhost:8080
```

token 文件可保存纯 JWT，或 `HATCHET_CLIENT_TOKEN=...`。脚本拒绝 API 重定向；失效 token、租户不匹配、错误数据库或缺失迁移均返回非零退出码。
