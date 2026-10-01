# OLAP 可见性延迟对照

Grafana 面板：<http://localhost:3000/d/hatchet-olap-latency>
面板 JSON：`hack/dev/grafana/olap-latency.json`

## 测量口径

探针从 REST trigger 返回 run ID 后开始计时，并发请求 task 与 workflow-run 详情，任一返回 200 时记录可见性
它模拟控制台的 `return_only_id=true` 和两路详情查询，包含读取请求耗时及 20 ms 轮询间隔
这不是 CK 副本复制延迟，也不是任务执行耗时；任务是否完成另行核对

每个后端使用新建的 PG 数据库、RabbitMQ vhost，CK 使用专用数据库和 Keeper root
PG 测量与 CK 测量按顺序进行，不与集成测试并行运行
两边使用相同 API、engine、controller 二进制及仓库官方 Go loadtest worker，启用 Prometheus、关闭 security check 和采样、关闭客户端 TLS
单并发、目标每秒 1 次、1 KiB 输入，5 次预热不计入统计，再测量 50 次
统计使用原始样本的 nearest-rank 分位数，50 个样本的 p99 即最大值，不作为生产尾延迟估计

```sh
python3 hack/dev/test-olap-latency.py \
  --samples 50 --warmup 5 --interval 1 \
  --output /tmp/hatchet-olap-latency
```

脚本读取根目录 `.env`，要求 PG、RabbitMQ、CK、Keeper 均已配置
测试使用 API 8081、gRPC 7071、健康检查 8741/8742、指标 19101/19102、探针 19103
测试结束或失败时删除自身 PG 数据库和 RabbitMQ vhost，清理自身进程组，保留 CK 测试数据库及 Keeper root
日志存放在仅当前用户可读的输出目录，不自动输出认证信息
本脚本依赖本机 dbx 容器命名及管理员权限，不用于生产库

## 新增指标

| 指标 | 含义 |
|---|---|
| `hatchet_olap_visibility_probe_duration_seconds{backend,phase}` | 客户端探针，phase 包含 `trigger_request`、`return_to_visible`、`trigger_to_visible` |
| `hatchet_olap_created_to_published_seconds{backend,kind}` | 核心 inserted_at 到成功创建调用返回，包含队列延迟，排除失败和未取得锁的记录 |
| `hatchet_olap_write_duration_seconds{backend,operation,result}` | 创建任务、DAG 和任务事件的仓储调用耗时，包含协调与提交，成功和失败分开 |
| `hatchet_olap_publication_phase_duration_seconds{phase}` | CK 的 stage、publication_lock、commit_publish 阶段耗时，包含失败尝试 |

仓储创建指标在重复投递成功时可能再次计数，也不保证详情 API 此刻已满足所有关联查询条件，不能与 HTTP 探针指标互换
标签不包含 tenant、run ID 或任务 ID
通过仓储装饰器注册 PG/CK 公共指标，业务返回值及锁失败集合直接透传
CK 阶段指标仅增加计时，不修改发布、恢复和可见性协议

Prometheus 通过 dbx 的 `hatchet-local` file-SD job 抓取本机 controller、engine 和测量探针
测试结束后移除测试目标，正常组件保留 controller 9091、engine 9092
服务使用 `SERVER_PROMETHEUS_ENABLED=true` 并重启后才会输出新增仓储指标，未重启的现有进程不具备新指标
Grafana 探针 stat 使用时间窗口内最后一次抓取的累计分布；分位数为直方图估算，准确分位数见下方原始样本统计

## 2026-09-29 实测结果

正式对照：2026-09-29，本机现有共享基础设施；每个后端 5 次预热 + 50 次正式样本，目标 1 次/s，输入 1 KiB

| 指标（ms） | PG | CK |
|---|---:|---:|
| 返回 ID → 详情可读 平均 | 29.1 | 772.0 |
| 返回 ID → 详情可读 p50 | 31.9 | 638.6 |
| 返回 ID → 详情可读 p95 | 35.3 | 1708.1 |
| 返回 ID → 详情可读 p99 / 最大 | 37.3 | 1907.8 |
| 触发开始 → 详情可读 平均 | 56.9 | 785.5 |
| 触发开始 → 详情可读 p50 | 53.7 | 651.0 |
| 触发开始 → 详情可读 p95 | 76.5 | 1721.0 |
| 触发开始 → 详情可读 p99 / 最大 | 80.5 | 1916.8 |

PG 初始 404：43/50，CK：50/50；探针立即查询，比实际页面跳转更早，不能将该比例当作控制台错误率
两边各 55 个任务全部完成，表中的分位数由原始样本计算。50 样本的 p99 等于最大值，不代表生产 p99

仓储指标（包含预热）：create_tasks 平均 PG 4.44 ms / CK 502.94 ms；创建到发布平均 PG 21.48 ms / CK 592.77 ms
CK 所有 publication 的阶段平均：stage 40.09 ms、publication_lock 191.33 ms、commit_publish 85.77 ms。阶段覆盖所有实体发布，不可直接相加解释单个任务耗时

原始样本和指标快照：`/tmp/hatchet-olap-latency-final-20260929`


## 2026-09-29 CK 优化复测

仅修改新增 CK 实现，正常提交复用全副本同步 ACK，不重复执行恢复的副本同步与数据回读；已有 pending 仍进行完整恢复校验
状态加载由四次查询合为一次，trace 去重仅查询本次输入的键，Keeper 所有权、发布序列及确认规则保持有效

复测沿用上述条件，两个后端各 5 次预热 + 50 次正式样本，均 55/55 完成

| 返回 ID → 详情可读（ms） | PG 本轮 | CK 优化前 | CK 优化后 |
|---|---:|---:|---:|
| 平均 | 29.1 | 772.0 | 380.2 |
| p50 | 33.1 | 638.6 | 360.9 |
| p95 | 38.4 | 1708.1 | 462.0 |
| p99 / 最大 | 41.2 | 1907.8 | 844.1 |

CK 平均降低 50.8%，p95 降低 72.9%；仍高于 PG，尚未达到 PG 的可见性延迟
CK create_tasks 平均 150.95 ms（优化前 502.94 ms），创建到发布平均 184.57 ms（优化前 592.77 ms）
所有 publication 的阶段平均：stage 41.66 ms、publication_lock 16.72 ms、commit_publish 28.26 ms
这些阶段平均覆盖所有实体，并非同一个 task 的分解
PG 初始 404 40/50，CK 50/50；优化后仍有异步可见性窗口，不能承诺触发后立即可读

本机共享基础设施、单副本、低频触发结果；尚未验证生产吞吐及多副本故障切换
原始样本及指标快照：`/tmp/hatchet-olap-latency-optimized-20260929`

优化后 `go test -race ./pkg/repository/clickhouse`（含 PG/CK 共用契约与 ACK 丢失故障）及 `go vet` 通过
2026-09-30 官方完整 loadtest 复验：18 个事件生成 51 个 run，全部完成；事件关联、状态统计、输入输出、task events、trace 和重复日志检查通过
延迟测试和端到端测试进程已退出，端口释放；延迟测试 PG 数据库、RabbitMQ vhost 和临时 Prometheus 目标已清理，CK 测试数据保留


## 2026-09-30 第二轮：详情读取优化

仅修改新增 CK 包的 task/workflow 读取及其回归测试，不修改官方 API、controller、调度或 SDK
已完成独立任务的 `ReadTaskRunData` CK 读取由约 8 次减到 5 次，`ReadWorkflowRun` 由约 8 次减到 4 次
事件在同一发布快照中复用，按最终所选重试读取 payload；子任务统计合并 task/DAG 查询，排除 DAG 内步骤
所有权、发布 ACK、pending 恢复规则保持有效

本轮顺序：先测读取优化版，再在同一运行环境通过 Go 编译 overlay 恢复两个 CK 文件的上一版读取路径作对照
overlay 不修改工作区；两次测试均保持本地现有服务运行，各后端新建隔离 PG 数据库、RabbitMQ vhost 与 CK 数据库
共享基础设施存在背景活动，不能视为无噪声基准；每组 5 次预热 + 50 次正式样本，各 55/55 完成

| 返回 ID → 详情可读（ms） | PG 读取优化轮 | CK 同环境上一版 | CK 读取优化版 |
|---|---:|---:|---:|
| 平均 | 28.6 | 400.3 | 334.8 |
| p50 | 32.5 | 352.4 | 312.7 |
| p95 | 37.4 | 835.2 | 575.6 |
| p99 / 最大 | 39.3 | 1155.9 | 762.9 |

相对本轮上一版补测，平均降低 16.3%，p95 降低 31.1%
前一轮历史 p95 是 462 ms，本轮新版是 576 ms，因此不能声称相对所有历史场景尾延迟均改善
50 个样本不足以估计生产 p99，异步可见性窗口仍然存在

原始样本、指标快照及两个对照源码副本：

- `/tmp/hatchet-olap-latency-read-optimized-20260930`
- `/tmp/hatchet-olap-latency-read-baseline-20260930`

`go test -race ./pkg/repository/clickhouse` 含 PG/CK 共用契约、ACK 丢失及缺失 pending 数据故障通过
新增验证覆盖固定快照、历史重试输出、DAG 详情和子任务计数；`go vet` 通过

官方六类 loadtest 在端口偏移 100 的新编译服务上通过：18 个事件生成 51 个 run，全部完成
事件关联、状态统计、输入输出、task events、trace 和重复日志检查通过
测试进程及临时 PG 数据库、RabbitMQ vhost、Prometheus 目标已清理；现有本地服务继续运行，需重启 API 加载读取优化
Grafana 第 2–4 个 stat 面板固定到读取优化测试结束后的抓取时刻，其他曲线按选定时段展示；避免随后 overlay 补测覆盖新版统计

## 2026-09-30 TiDB 验证

TiDB 后端通过官方六类 loadtest：18 个事件、51 个完成的 run；API 核对事件关联、状态统计、payload、task events、trace 和重复日志
隔离 PG、TiDB 数据库及 RabbitMQ vhost，启动设置 `SERVER_SECURITY_CHECK_ENABLED=false`，结束后清理测试进程与隔离资源

可见性测试采用同一脚本、每后端 10 次预热 + 1000 个正式样本，约 10 次/s，详情轮询 5 ms
两后端均 1010/1010 完成，原始样本在 `/tmp/hatchet-tidb-latency-1000-rerun-20260930`

| 返回 run ID → 详情可读 | PG | TiDB |
|---|---:|---:|
| p50 | 16.2 ms | 44.7 ms |
| p95 | 20.7 ms | 97.6 ms |
| p99 | 33.4 ms | 128.6 ms |

TiDB p95 未达到 40 ms 切换门槛，当前服务保持原有后端配置
Controller 指标显示 `create_tasks` 调用平均 PG 4.25 ms / TiDB 9.82 ms，核心创建到 OLAP 发布平均 PG 11.99 ms / TiDB 39.14 ms，说明除了 TiDB 事务耗时，控制器排队与锁争用也扩大了可见性窗口
TiFlash 选择性副本和查询计划在独立集成测试库验证通过；这组探针测的是 run 详情点查，不能用于证明 TiFlash 统计查询性能

## 2026-09-30 TiDB v2 实施与验收

结构化 v2、事务回执、乱序补偿、attempt 投影、SQL 分页/聚合、日志、trace、外部 payload 和 UTC 分区维护已实现
最终构建功能 race 套件 23 个顶层测试通过（31.374s），47 个 OLAP 方法及日志 3 个方法有共用 PG 契约调用；20,001 条边界 race 单独通过（55.000s）
任务、run、事件列表的 `total` 与 PG 一致，封顶 20,000；状态统计使用全量计数，metadata 为顶层文本 OR，无匹配记录返回空数组
最终源码/schema/构建校验记录在 `/tmp/hatchet-tidb-v2-build-final.json`

最终构建再次通过官方六类 loadtest：18 个事件、51 个完成 run、183 个完成 task、33 个 DAG
核对 895 条 task events、2 条重复内容日志、431 条 trace、412 条 payload，pending 为 0；核心任务、DAG、事件数量一致
安全检查显式关闭，隔离库、RabbitMQ vhost、测试进程和端口已清理
结果：`/tmp/hatchet-tidb-v2-e2e-final-5/result.json`

早期 v2 构建的三组正式对照如下，每后端 10 次预热 + 1000 个正式样本，轮询等待 5 ms、目标触发间隔 100 ms
这批探针以 task/workflow 两详情中的任意一个 200 为可读，均未达到 v2 的 40 ms 门槛；它们不能作为最终构建通过的证据

| 返回 ID → 首个详情可读 p95 | PG | TiDB v1 | TiDB v2 |
|---|---:|---:|---:|
| 第 1 组 | 22.4 ms | 172.4 ms | 218.7 ms |
| 第 2 组 | 26.4 ms | 441.5 ms | 296.4 ms |
| 第 3 组 | 89.2 ms | 635.5 ms | 353.0 ms |

原始记录：`/tmp/hatchet-tidb-v2-final-group{1,2,3}`
延迟包含 HTTP 读取耗时和轮询等待，不等于数据库提交时间；5 ms 是轮询等待间隔，不是服务端可见时刻的绝对测量误差

最终探针要求 task 与 workflow 两详情均返回 200，再独立核对 1010 条全部完成
PG 的 1000 个正式样本 p95 为 29.8 ms；随后 v1 对照在 TiKV 90.27% 内存时停止，v2 独立补测也在 TiKV 96.05% 内存时停止，未得到完整正式样本
这些中断数据不能用于证明最终 v2 达标，后续组未启动，现有 DSN 未切换
记录：`/tmp/hatchet-tidb-v2-current-group1`、`/tmp/hatchet-tidb-v2-current-only-group1`

规模脚本：`hack/dev/test-olap-scale.py`、`hack/dev/benchmark-olap-scale.go`
PG 的一万、十万、一百万 run、20,000 封顶列表计数、全量状态统计和 10/10,000 历史事件写入全部通过，最终真实事件计数 10110
结果：`/tmp/hatchet-tidb-v2-scale-pg-final/postgres.json`
其中前两个规模的部分时间段与小型契约验证重叠，记录用于功能规模验收，不作为公平性能排名
TiDB 早期规模测试仅完成一万行，随后在五万行附近触发内存保护；TiDB v1/v2 的完整十万、百万及历史增长性能验收仍未完成
早期一万行 EXPLAIN ANALYZE 的优化器/TiFlash 路径报 MPP 内存限制错误，TiKV 路径成功；未添加固定 TiFlash 提示

本机资源：Docker VM 8 CPU / 11 GiB，TiDB 1 GiB、TiKV 2.5 GiB、TiFlash 5 GiB
扩展测试发生过 TiDB OOM（Docker `oom`、退出 137、自动重启），此后补充内存保护，所有测试流量已停止，没有主动重启或修改 dbx
压力期间 TiDB 空闲 CPU profile 的 `gcDrain` 占累计 CPU 94.67%，heap 约 715 MB，分区表元数据和 coprocessor 缓存占较大部分；这是压力证据，不能据此单独归因为分区设计
停止后 TiKV 仍约 96.7% 内存，PD 报 6675 个 Region；隔离库已经删除，但逻辑删除不会立即消除所有存储资源占用，Region 合并与回收由集群管理，见 [PingCAP Region 资源说明](https://docs.pingcap.com/tidb/v8.4/massive-regions-best-practices/)
未调整 GC、Region 调度、缓存、节点配额或副本拓扑；完整性能验收需要有足够余量的环境

[TiDB v2 Grafana](http://localhost:3000/d/hatchet-tidb-olap-v2/hatchet-tidb-olap-v2) 已导入 13 个面板，区分请求路由与采样核验执行引擎
隔离测试使用 `--no-prometheus`，不修改 dbx 抓取配置，原始 JSON 和 `.prom` 文件为本轮测试依据；面板实时数据依赖已配置服务的抓取

功能验收通过，完整性能验收未通过/未完成，当前不允许切换；v2 只接受新空库，旧 v1 库与构建保留
