# TiDB OLAP v2

核心调度继续使用 PG，OLAP 默认仍为 `postgres`，另可选择 `clickhouse`、`tidb`
TiDB 使用 `database/sql`、MySQL 驱动与显式 SQL，API、Controller、MQ、前端和 SDK 的业务接口保持不变

## 存储与事务

- `v1_tasks_olap`、`v1_dags_olap` 保存静态字段；`v1_runs_olap` 保存当前状态、列表字段和 DAG 差量计数
- `v1_task_attempts_olap` 保存各重试的时间、错误与输出选择投影；`v1_task_events_olap` 保存事件历史
- payload、日志、trace 使用独立领域表；UUID 为 `BINARY(16)`，时间为 UTC `DATETIME(6)`，payload 保留原始字节
- lookup 先定位创建时间并裁剪分区；metadata 以带类型和父子关系的节点存储，AND 编译为 SQL EXISTS，OR 使用 PG 顶层文本语义
- run 锁按 UUID 排序，同批使用同一连接和悲观事务；锁身份初始化为协调元数据，业务、状态、关联、初始化标记与提交回执同事务提交
- 首次 standalone 写入可以跳过空状态读取；乱序事件已初始化的 run 仍读取重试投影并归并，待处理记录持久化并限量补偿
- 提交 ACK 不明确时按回执确认；日志以 sequence 分配 ID，两个内容相同的调用保留两条，成功返回前完成持久提交
- 历史表使用 UTC 日分区，维护保留窗口及未来两天；完整过期分区删除，边界按索引分批删除
- payload 上传在事务外完成，上传确认后再提交索引和游标；失败保留原数据，非空 PG DBTX 明确报错

列表过滤、计数、排序、分页均在 SQL 内完成，再按当前页批量填充，读取拼装使用短快照事务
任务、run、事件列表的 `total` 按接口契约最多返回 20,000；状态分组和时间桶统计使用全量精确计数
状态写入读取受影响任务及 attempt 投影，不枚举历史事件或全租户 payload

## 新库初始化

v2 只支持空库初始化，不升级 v1，不回填历史；保留旧库和旧构建
迁移记录 SQL 版本与 SHA256，服务启动检查表、列类型、字符集、排序规则、索引、分区键、sequence 和校验值

```sh
DATABASE_OLAP_BACKEND=tidb
DATABASE_TIDB_DSN='user:password@tcp(127.0.0.1:4000)/hatchet_olap_v2'
go run ./cmd/hatchet-migrate-tidb --tiflash
```

| 配置 | 默认 |
|---|---|
| `DATABASE_TIDB_WRITE_CONCURRENCY` | 8 |
| `DATABASE_TIDB_MAX_OPEN_CONNS` / `MAX_IDLE_CONNS` | 40 / 10 |
| `DATABASE_TIDB_QUERY_TIMEOUT` | 5s |
| `DATABASE_TIDB_TIFLASH_QUERY_TIMEOUT` | 500ms |
| `DATABASE_TIDB_CONN_MAX_LIFETIME` | 15m |

## TiFlash 与观测

只为 `v1_runs_olap`、`v1_log_line` 建副本；详情、trace ID 和日志分页走 TiKV，聚合允许优化器选择引擎
列式错误或语句超时在同一快照、剩余请求预算内回退 TiKV；请求取消和非存储错误不掩盖
当前不固定 TiFlash 提示，代表性数据验证前不据表类型推断引擎

[Grafana dashboard](http://localhost:3000/d/hatchet-tidb-olap-v2/hatchet-tidb-olap-v2) 展示阶段延迟、连接池等待、冲突、回退和副本进度
请求路由与每分钟最多一次的 EXPLAIN ANALYZE 核验分别记录，未核验引擎为 unknown

## 验收

[方法覆盖矩阵](IMPLEMENTATION.md) 区分共用 PG 契约、故障注入、运行验证和性能门槛

```sh
TIDB_TEST_TIFLASH=1 python3 hack/dev/test-tidb-contract.py --race
python3 hack/dev/test-tidb-e2e.py --port-offset 20 --output /tmp/hatchet-tidb-e2e
python3 hack/dev/test-olap-latency.py --backends postgres,tidb --samples 1000 --warmup 10 --poll-ms 5 --interval 0.1 --no-prometheus --output /tmp/hatchet-olap-compare
```

测试使用隔离 PG/TiDB 数据库、RabbitMQ vhost 和端口，显式关闭 security check；结束清理自身进程和资源，不修改 dbx
正式可见性对照须每后端 1000 个样本并重复三组，三组 p95 均 ≤ 40 ms 才允许切换

功能共用契约、race、20,001 条边界和官方六类 E2E 已通过，性能切换门槛尚未通过，现有 DSN 未修改
规模脚本 `hack/dev/test-olap-scale.py` 使用一万、十万、一百万 run 的固定数据，以及 10/10,000 历史事件增长对照；测试脚本带共享容器内存保护
性能结果与本机资源限制见 [测量记录](../../../hack/dev/README.olap-latency.md)
切换须暂停新触发和定时触发、等待在途 run 完成并清空 OLAP 队列与日志缓冲，再统一更新 API/Controller/Engine DSN
旧库与旧构建用于回退，新旧历史不会自动合并；本机单 TiKV 结果不能代替生产副本拓扑验证
