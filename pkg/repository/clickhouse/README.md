# ClickHouse OLAP 与日志后端

默认后端仍为 PostgreSQL，`DATABASE_OLAP_BACKEND=clickhouse`（也接受 `ck`）同时切换 OLAP 和 v1 日志仓储
核心任务、调度、配置和身份数据继续使用 PG；CK 不可用时启动失败，不回退到 PG OLAP

## 启用

先配置现有 PG、RabbitMQ、CK 和 Keeper 基础设施，再独立运行 CK 迁移
迁移命令不加载 `.env`，不运行 PG 迁移，不启动服务

```sh
export DATABASE_CLICKHOUSE_ADDRESSES=127.0.0.1:9009
export DATABASE_CLICKHOUSE_KEEPER_ADDRESSES=127.0.0.1:9181
export DATABASE_CLICKHOUSE_DATABASE=hatchet_olap
export DATABASE_CLICKHOUSE_KEEPER_ROOT=/hatchet/olap/hatchet_olap
# 通过环境或配置文件设置用户名、密码和认证，勿提交密钥
# DATABASE_CLICKHOUSE_USERNAME / DATABASE_CLICKHOUSE_PASSWORD
# DATABASE_CLICKHOUSE_TLS / DATABASE_CLICKHOUSE_KEEPER_AUTH
go run ./cmd/hatchet-migrate-clickhouse
export DATABASE_OLAP_BACKEND=clickhouse
task all
```

API、engine 和 controller 必须使用相同后端、数据库和 Keeper root，并在切换后重启
Keeper root 专用于该 CK 数据库，CK 服务须配置 `{shard}` / `{replica}` 宏
多个 CK 地址必须属于同一复制分片，启动时检查表版本、列类型、引擎和复制路径
切换只影响新写入，不自动复制历史 PG OLAP 数据；切回 PG 也不会复制 CK 中的新数据

## 存储与兼容规则

实现全部 47 个 `OLAPRepository` 方法和 3 个日志方法，通过接口断言检查方法覆盖
业务类型保留现有 repository 类型，新增实现集中在本包；配置与 loader 负责选择后端，不修改调度和 OLAP controller

| CK 表 | 用途 |
|---|---|
| `entities` | 任务、DAG、监控事件、用户事件关联、payload、trace 和诊断记录的版本 |
| `manifests` | 批次行数及摘要 |
| `commits` | 已授权批次的提交序列 |
| `log_lines` | v1 日志及任务信息 |
| `schema_version` | 迁移版本 |

Keeper 提供临时所有权证明、发布序列和待发布恢复
读查询先限制已发布批次，再选择实体最新版本，避免后台合并提前暴露未提交状态
任务状态按重试次数和状态优先级对账，支持事件先到、DAG 汇总、重复写入及乱序到达
状态同步发布，因此 PG 的临时状态更新队列在 CK 后端为空
注册的复制节点全部在线并确认写入后才发布，多副本不可用时停止发布
正常路径在 data 与 manifest 全副本同步 ACK 后授权，写入 commit 并推进 Keeper head
只有恢复已有 pending 批次时执行副本同步、行数和摘要回读校验；ACK 丢失不会直接推进 head
状态加载在同一发布序列下合并查询 task、DAG、task event 和 payload，trace 去重只查询本次输入的键
任务/工作流详情复用同一快照的事件，按所选重试加载最终 payload；子任务统计合并 task/DAG 查询，排除 DAG 内步骤
Task 读取固定单个发布快照，避免多次 Keeper head 查询及混用发布序列

payload 索引和卸载元数据存储在 CK，外部对象存储复用现有配置
保留策略按 UTC 日边界过滤读结果并清理过期实体和日志
提交记录、manifest 和未授权的遗留批次当前不自动回收，需要后续单独设计安全 GC，避免删除仍可恢复的数据

日志以 5 ms / 512 条 / 1 MiB 目标聚合，调用返回前等待同步 ACK
内部重试复用 ID，业务上两次相同日志保留两条；超出 1 MiB 的单条记录独立发送，接口没有单条 metadata 大小限制
工作流摘要时间遵循监控记录的入库时间，任务执行 timing 使用事件时间，两者可能因监控乱序而不同

## 验证

可复跑端到端脚本读取根目录 `.env` 与环境变量，显式禁用 security check，使用已有基础设施和已初始化的 tenant
它编译并启动 API、调度/gRPC、controller 和官方 Go worker，运行仓库官方 `hatchet-loadtest` 的全部六类事件
结束或失败时清理自身进程组，测试数据保留在所配置的数据库

```sh
python3 hack/dev/test-clickhouse-e2e.py
# 与现有本地服务共存时，测试端口整体偏移 100
python3 hack/dev/test-clickhouse-e2e.py --port-offset 100
```

脚本要求 CK 地址、Keeper 地址和认证配置，默认使用开发 seed tenant，可通过 `HATCHET_CK_E2E_TENANT_ID` 指定 tenant
它检查 18 个事件产生的 51 个 run 全部完成，并检查 API 的事件关联、状态统计、输入输出、task events、trace 和重复日志
官方 loadtest 可能因监控到达时序报告 timing 样本不足；脚本额外核对最终 run 数和状态，不能仅以 loadtest 退出码代替完整性检查

集成测试创建随机 CK 数据库与 Keeper root，并删除自己的测试资源，不重启共享基础设施

```sh
HATCHET_CLICKHOUSE_TEST=true \
CLICKHOUSE_TEST_USERNAME=default \
go test -race ./pkg/repository/clickhouse ./pkg/config/database ./pkg/config/loader
```

连接变量：`CLICKHOUSE_TEST_ADDRESS`、`CLICKHOUSE_TEST_PASSWORD`、`KEEPER_TEST_ADDRESS`
设置 `CLICKHOUSE_CONTRACT_POSTGRES_URL` 后还运行 PG/CK 共用读契约，涵盖 payload、列表、状态指标、任务事件和用户事件关联
故障测试涵盖合并后的发布可见性、提交标记后恢复、所有权 ABA、会话超时重连、缺失待发布数据、查询超时、日志与发布 ACK 丢失、正常路径无恢复 I/O 及 payload 卸载恢复

实测环境是 CK 26.9.5.2、单节点 Keeper 和现有 PG/RabbitMQ
已验证功能端到端、进程重启后读取和默认 PG 回归；多节点故障切换、大数据量查询及生产吞吐尚未验收

### 本地执行记录（2026-09-29）

| 检查 | 结果 |
|---|---|
| 官方全部六类事件，包含 durable 子任务和复杂/嵌套 DAG | 一轮 18 个事件生成 51 个 run，全部完成 |
| 已有 CK 数据重启后读取，再运行官方 loadtest | 58 个已有 run 保留，累计 109 个 run 全部完成 |
| 默认 PG，CK/Keeper 地址故意设为不可用 | 官方普通任务 2/2 完成，证明默认路径不依赖 CK |
| CK 场景的 PG 写入隔离 | 核心任务 373 条，PG OLAP 任务 0 条，PG 日志 0 条 |
| PG/CK 读契约及故障测试 | `go test -race` 通过 |
| 新包静态检查与服务入口编译 | `go vet` 和相关 `go test` 通过 |

测试进程按进程组清理，保留共享 dbx 基础设施和测试数据
