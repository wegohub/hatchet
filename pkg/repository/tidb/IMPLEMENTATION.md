# TiDB v2 验收记录

PG 是行为契约，以下矩阵记录共用测试的实际调用；调用覆盖不等于所有输入空间的兼容性证明

| OLAP 方法 | 共用契约 |
|---|---|
| `UpdateTablePartitions` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `SetReadReplicaPool` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ReadTaskRun` | [v2_pg_contract_test.go](v2_pg_contract_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `ReadWorkflowRun` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ReadTaskRunData` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `ListTasks` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_count_test.go](v2_pg_count_test.go) |
| `ListWorkflowRuns` | [v2_pg_contract_test.go](v2_pg_contract_test.go)、[v2_pg_count_test.go](v2_pg_count_test.go) |
| `ListTaskRunEvents` | [pg_contract_integration_test.go](pg_contract_integration_test.go) |
| `ListTaskRunEventsByWorkflowRunId` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ListWorkflowRunDisplayNames` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ReadTaskRunMetrics` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_contract_test.go](v2_pg_contract_test.go)、[v2_pg_count_test.go](v2_pg_count_test.go) |
| `CreateTasks` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_contract_test.go](v2_pg_contract_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `CreateTaskEvents` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_contract_test.go](v2_pg_contract_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `CreateDAGs` | [v2_pg_contract_test.go](v2_pg_contract_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `GetTaskPointMetrics` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `UpdateTaskStatuses` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `UpdateDAGStatuses` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ReadDAG` | [v2_pg_contract_test.go](v2_pg_contract_test.go)、[transitions_pg_test.go](transitions_pg_test.go) |
| `ListTasksByDAGId` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ListTasksByIdAndInsertedAt` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ListTasksByExternalIds` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `GetTaskTimings` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `BulkCreateEventsAndTriggers` | [pg_contract_integration_test.go](pg_contract_integration_test.go) |
| `ListEvents` | [pg_contract_integration_test.go](pg_contract_integration_test.go)、[v2_pg_count_test.go](v2_pg_count_test.go) |
| `GetEvent` | [pg_contract_integration_test.go](pg_contract_integration_test.go) |
| `GetEventWithPayload` | [pg_contract_integration_test.go](pg_contract_integration_test.go) |
| `ListEventKeys` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `GetDAGDurations` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `GetTaskDurationsByTaskIds` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `GetTaskStartedTimestamps` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `CreateIncomingWebhookValidationFailureLogs` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `StoreCELEvaluationFailures` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `PutPayloads` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `ReadPayload` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `AnalyzeOLAPTables` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `OffloadPayloads` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `PayloadStore` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `StatusUpdateBatchSizeLimits` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ListWorkflowRunExternalIds` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ProcessOLAPPayloadCutovers` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `CountOLAPTempTableSizeForDAGStatusUpdates` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `CountOLAPTempTableSizeForTaskStatusUpdates` | [v2_pg_logs_test.go](v2_pg_logs_test.go) |
| `ListYesterdayRunCountsByStatus` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `CreateSpans` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `ListSpansByTraceId` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `CreateSpanLookupTableEntries` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |
| `LookUpTraceId` | [v2_pg_contract_test.go](v2_pg_contract_test.go) |

日志 `PutLog`、`ListLogLines`、`GetLogLinePointMetrics` 位于 `v2_pg_logs_test.go`

已验证：过滤、分页与契约计数、空列表、Unicode、嵌套 metadata 与精确数字、不同接口的时间/输出选择、durable 恢复、重试和 operator DAG
任务、run、事件列表的 `total` 上限为 20,000，状态统计不封顶；20,001 条共用测试同时检查上限和过滤后计数
状态统计的 metadata 使用顶层文本 OR，未匹配记录返回空数组；日志按 external ID 过滤时从核心 PG 仓储定位任务，支持 OLAP 副本尚未存在的情况

故障/并发测试：`v2_fault_integration_test.go` 验证乱序、重放、多 Controller NOWAIT、过期锁缓存、提交 ACK 丢失及后续事件覆盖后的回执确认；`payload_integration_test.go` 验证外存上传 ACK 丢失后原始 payload 保留与恢复

`analytics_fault_test.go` 使用驱动故障注入验证同一快照和剩余预算回退，包括嵌套计数子查询的 TiKV 提示位置
真实 TiFlash 集成测试检查 AVAILABLE/PROGRESS 和 EXPLAIN 规划；查询采样与规模脚本执行 EXPLAIN ANALYZE，执行失败时标记 unknown，不据静态规划宣称列式查询性能

大页回归与并发日分区维护均使用真实 TiDB，保留期测试验证初始化标记不会先于仍保留的状态被删除

最终 schema 的功能 race 套件通过（31.374s），20,001 条 PG/TiDB 边界 race 独立通过（55.000s）
schema 漂移测试检查缺失索引、字符集/排序规则不兼容；v1 拒绝直接升级
最终构建通过官方 default、batch、durable、dag、dag-shapes、dag-nested 六类 E2E：18 个事件、51 个完成 run、183 个完成 task、33 个 DAG，pending 为 0
同时核对日志、trace、payload、事件关联和核心数据数量，测试进程、端口、隔离库及 RabbitMQ vhost 清理完成

PG 的一万、十万、一百万规模及 10/10,000 历史事件对照已通过；TiDB 完整规模与三组最终可见性切换门槛仍需以测量记录为准
本机 TiDB 限额为 1 GiB，扩展测试期间发生过一次 OOM 自动重启，随后清理隔离资源并补充内存保护；没有主动重启或修改 dbx

v1 源码与 API/Engine/迁移构建已独立保留，现有 DSN 和旧库未修改
