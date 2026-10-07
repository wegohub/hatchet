# Claude 评审问题处理记录：SDK 0.1.9

输入：[code-review-report.md](code-review-report.md)。修复限定于 `sdks/wego/**`，协议仍为 3，MQ 仍为 PostgreSQL。公开包的导入路径和现有方法保留；新增实例级结果轮询配置与流取消控制预算。

报告中的代码片段、测试名称和日期并非全部对应当前源码。本记录按问题逐项核对，不把建议代码直接当作可执行补丁，也不沿用“生产级”“可以接受的延迟”等未经负载与业务要求验证的结论。

## 逐项处理

| 报告问题 | 处理与当前规则 | 验证 |
| --- | --- | --- |
| 1.1.1：取消后等 ACK 可能泄漏 | 官方 listener 的 Stop、CleanupTaskState 已会交付错误 ACK；新增根生命周期退出分支，并在获得确认权后再次检查根状态。活跃 invocation 不能以任意超时释放确认槽 | `TestCancelledAckExitsWithInvocation` 覆盖 Stop、清理、根取消；既有 `TestCancelledAckDrainsBeforeNextRequest` 验证迟到 ACK 不串位 |
| 2.1.1：握手稳定性 | 独立监听 READY 与 PING 结果；匹配 READY 后取消并回收 PING 等待，再提交 OPEN。同一会话最多一个在途 PING，重发间隔 250ms、500ms、随后 1s；总次数受握手 deadline 限制 | `TestReadyDoesNotWaitForPingResult`、`TestHandshakePingsAreBounded`、RUN 黑洞预算测试及真实流场景 |
| 2.4.1：流清理预算与诊断 | CANCEL 默认独立 5 秒预算，受实例关闭预算上限约束；新增 `StreamConfig.CancelTimeout`，零值使用默认值。握手和清理记录阶段耗时、会话/方法及 gRPC code，不记录 payload、metadata、token | `TestCancelControlUsesCleanupBudget`；订阅与结果退出屏障测试仍保留 |
| 3.1.1：无法定位未退出 I/O | 内部 BeginIO 登记稳定类别、编号与开始时间。清理到期从锁内取得快照，在锁外记录数量和具体操作；返回错误保留预算原因。关闭按调用排空、SDK I/O 确认拆分为辅助函数 | `TestUnfinishedIOHasDiagnostics`、`TestOwnedIOCleanupPrecedesBackendRelease`、并发关闭测试 |
| 5.2.1：model 混合多类声明 | 在同一公开包内按文件分离：`execution.go` 为执行 DTO，`policy.go` 为策略/枚举，`errors.go` 为错误和哨兵。避免新增 config/dto/errors 三个公开包及迁移成本 | 构建、类型图/API 隔离和现有错误/策略测试 |
| 6.3.1：未完成 memo 缺乏真实恢复证据 | 新增透明 gRPC 故障代理，只丢弃首次 `wego.now` CompleteMemo。真实引擎先登记 memo，Worker 重启实际收到已存在但空 payload 的 ACK，补算；再次重启验证补算时间持久复用 | `TestDurablePendingMemoRecovery`，观察 invocation 3、父子 RunID 复用、两次换 Worker、结果与资源清理 |
| 7.1：长关闭函数 | 按业务调用排空与 SDK 自有 I/O 退出的不同预算提取内部函数；保持单次关闭、资源所有权和释放屏障 | 正常、取消、预算到期、并发关闭回归 |
| 7.3.1：内部 core 命名 | 改为 `internal/client`、`internal/features`、`internal/telemetry`，包声明、导入、当前设计资料同步；公开 client/features/telemetry 导入路径保持原值 | 全 SDK 构建、生成与 API 隔离检查 |
| 8.1：记录 token 前缀 | 不增加 token 或前缀日志。诊断使用有限会话身份、操作类别、耗时和状态码，授权信息不参与日志 | 日志实现检查；无 token 新日志字段 |
| 9.1：并发流成本与适用性 | 新增四会话、每会话三条 256 字节消息的并发样本，保存每会话握手、全部往返、实际任务提交数和包含握手的总耗时；不设置无依据的消息频率边界 | `TestReviewConcurrentStreamingCost`，逐条内容/身份/EOF 断言、12 DATA/12 ACK、4 组初始化与 END |
| 9.1：RabbitMQ 性能对比 | 与本项目 PostgreSQL MQ 范围冲突，不引入 RabbitMQ；样本只描述实际验证的部署 | 报告 `mq=postgresql`，现有部署与 CI 范围不变 |
| 9.2：250ms 轮询放大 | 新增 `runtime.WithResultPollInterval`，默认 1 秒，零值采用默认，负值拒绝；在一次查询结束后再计时，避免慢查询累积补发。Conn 统一拥有配置，Worker 局部覆盖拒绝 | `TestResultPollingInterval`、配置预算测试、慢 REST 不阻塞完成结果测试、Worker 配置边界测试 |
| 10.1：结构化诊断 | READY 日志记录 PING 次数；握手阶段、CANCEL、传输清理分别记录耗时；I/O 超时记录快照。使用 Debug/Warn，不采样全局 goroutine 数替代实例资源跟踪 | 握手、清理预算及 I/O 诊断回归；真实会话清理断言 |

## 未直接采用的算法建议

- ACK 超时后释放锁会让旧确认被下一次操作误收。例如 Sleep 请求取消但仍在引擎执行，随后 Now 登记同一确认槽会收到 Sleep ACK。因此只在 ACK/官方清理或根退出后释放；根退出后不能重新登记。
- 原 `select` 的 100ms 分支不会让已经到达的 READY 必须休眠 100ms；PING 也不是重复 START。确实值得处理的是“前台等待 PING 提交/结果时无法及时处理 READY”和重复提交成本。本次针对这两个点改进，不能据此宣布历史两次超时根因已全部找到。
- 固定 100 次 PING 会对应用配置的长握手预算新增一个隐式截止。本次用最多一个在途请求、退避和既有总 deadline 同时约束队列负载与运行时间。
- token 前缀不是定位握手或关闭问题所必需的信息；不增加这类日志。吞吐推荐也不能直接写成“每秒低于 10 条”，应由业务延迟目标和实际部署测量决定。

## 配置与适用范围

```go
// 结果主订阅不变，辅助 HTTP 取消状态检查在每次查询结束后间隔 2 秒。
runtime.WithResultPollInterval(2 * time.Second)
```

外部取消状态的发现可能随轮询间隔延后；调用方 context 的 deadline/cancel、任务结果订阅不需要等待这次轮询。流 `CancelTimeout` 与握手预算不同，默认 5 秒，应用可通过完整 `runtime.StreamConfig` 配置。取消控制预算到期不能绕过订阅/结果 goroutine 的退出确认。

Worker 流适用于需要任务引擎调度、owner 会话和异步执行记录的交互。其每条输入 DATA、每条输出 ACK 都有任务调度成本；需要符合严格低延迟目标时，应测量现有网络 gRPC 入口与 Worker 入口，并据业务语义选择。当前没有验证生产长期负载、跨机器规模或自动流恢复。

## 验证证据

定向验证已通过：常规 unit/vet/race、真实 pending memo 两次恢复，以及四会话并发流样本。第一次 memo 故障 fixture 错将原始 key 当 JSON 导致连接中断，失败测试已回收 Worker 与定义；修正后重新执行通过，失败日志保留，未作为成功证据。

完整复验已通过，完成于 `2026-10-07T13:03:39.865221+00:00`：15 项质量门禁、28 个官方源文件/74 个片段、30 个示例场景/252 条必需断言、9 组流故障、真实引擎 race 与独立 embedded 全部通过。新增 pending memo 恢复和并发样本为验收运行器的必需证据。实际命令、运行身份和清理结果见 [验收报告](design/acceptance-report.json)，逐项处理和最终源码摘要见 [修复 JSON](claude-review-fixes.json)。0.1.7 的原始报告已另存为 [历史报告](design/acceptance-report-0.1.7.json)，其时间与结果不改写为当前版本。

本机日志与详细样本位于忽略的 `.test-results`；验收 JSON 提供可随源码交付的摘要、命令、断言、RunID/WorkerID 和清理结果。资源清理只针对唯一测试 namespace；执行历史及故意退出的 Worker 记录保留。托管 CI 未运行。

最终 race 并发样本：4 个会话、每会话 3 条 256 字节消息，握手范围约 4194～4218ms，往返最小/中位/最大约 1347/2471/2617ms，共提交 44 个任务（DATA/ACK 各 12，START/RUN/PING/OPEN/END 各 4）。样本显式使用 20 秒握手预算，SDK 默认仍为 10 秒；不计输出发布和辅助 REST 成本，不能据此给出生产吞吐保证。
