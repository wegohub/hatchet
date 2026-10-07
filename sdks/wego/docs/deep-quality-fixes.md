# wego SDK 0.1.10 深度评审处理报告（供 Claude 复审）

本报告对应 [DEEP_QUALITY_REVIEW.md](DEEP_QUALITY_REVIEW.md) 与 [OPTIMIZATION_PRIORITIES.md](OPTIMIZATION_PRIORITIES.md) 中的 Q-001～Q-011。11 项建议已经逐项核验处理，最终完整验收于 `2026-10-07T15:14:27.098397+00:00` 通过全部 15 门禁。部分原建议会破坏并发与取消语义，采用安全替代或保留必要规则，具体理由如下。所有改动限定于 `sdks/wego/**`，协议仍为 3；不修改 Hatchet 源码、根依赖或仓库 CI，也不使用 RabbitMQ。

## 逐项处理

| 问题 | 处理结果 | 实现与约束 | 回归证据 |
|---|---|---|---|
| Q-001：累计 ACK 全表扫描 | 已修复 | `releaseCredit` 只访问新增确认序号，O(k)；直接复用现有累计 ACK 位置，不增加重复状态。覆盖 Endpoint 输出、Client 输入订阅及 DATA 控制任务结果三条路径。所有入口先检查 ACK 不超过已发送位置，重复或回退 ACK 不重复释放。 | `TestCumulativeOutputACKBoundaries`、`TestCumulativeInputACKBoundaries`、`TestControlResultRejectsFutureACK`；窗口 64/1024 基准。 |
| Q-002：每次通知分配通道 | 使用安全替代方案修复 | `changeSignal` 在等待者检查条件的同一临界区内懒创建通道，广播后归零。无等待者时合并更新，通知零分配；有等待者时共享一代通道，继续广播到全部等待者。没有取消桥接 goroutine。 | `TestBroadcastRegistrationAndCancellation`、`TestBroadcastConcurrentWaiters`；通知有/无等待者基准；race 与满窗口真实故障验收。 |
| Q-003：控制分支手动解锁 | 已修复 | `updateControl` 统一加锁与 defer 解锁，分派到 DATA、END、ACK 的持锁状态处理。返回待发布 READY，`control` 在锁外调用后端。CANCEL 在锁内记录故障，没有取消与数据消费之间的状态间隙。 | `TestControlPublishesOutsideLock`；已有 OPEN、END 幂等与控制容量测试。 |
| Q-004：删除循环内故障检查 | 原建议不成立，保留并验证必要检查 | 故障检查与从缓存取出消息必须在同一把锁内，不能移到循环外或独立 select。否则失败已发生的会话仍可能交付缓存 DATA。等待取消时也优先返回已记录协议故障。继续每条实际消费发送 ACK，避免按八条批量确认在低流量或小窗口下停滞。 | `TestFailureDoesNotConsumeBufferedInput`、`TestWaitKeepsProtocolFailure`；客户端已有失败后不消费缓存的回归。 |
| Q-005：比较后分支解锁 | 已简化 | `acceptInput` 直接使用 `bytes.Equal`，不保存仅用于解锁后的临时布尔值；解锁由统一临界区负责。重复同内容幂等，内容冲突仍返回 DataLoss。 | 既有 `TestInputOrderingDuplicatesAndHalfClose`、`TestConflictingFramesAndWindow`。 |
| Q-006：JSON 重复转换 | 已修复 | `Decode` 对 string、[]byte、json.RawMessage 直接解析，结构化对象保持 JSON 回编码。带类型的 nil 字节和 RawMessage 仍遵循 null，非 nil 空文本保持非法。 | `TestSessionDecodeSources`，包含非法 JSON、错误结构、不可编码对象与 null；RawMessage 前后基准。 |
| Q-007：append 复制容量 | 已调整并验证所有权 | `copyPayload` 使用精确切片容量的 make+copy，应用于双向 DATA 缓存及最终响应副本。保留 nil 与非 nil 空切片区别，绝不借用调用者或解码器的可变缓冲。切片容量减少不等于分配器实际占用字节减少。 | `TestPayloadCopyAndBoundedCapacity`；253/4097 字节复制对照基准；载荷变换真实往返。 |
| Q-008：重复、机械注释 | 已改善触及路径，保留中文覆盖要求 | 控制、通知、等待、状态分组和新测试说明当前不变量、取消竞态、窗口预算及具体序号例子。没有按原建议删除私有声明和局部变量的中文说明；该要求来自用户，且由 AST 门禁检查。此轮不把其他模块的大量既有注释机械改写为新的模板。 | `TestDeclarationsHaveChineseComments`；控制与等待源码可读性复查。 |
| Q-009：重复等待 select | 已提取 | `waitForChange` 合并广播与 context 等待；Endpoint `wait` 补充首个故障优先级；Client 发送窗口复用该辅助函数。Client 接收的终态缺口计时器保留专用 select，避免丢失超时条件与计时器清理。 | 广播、取消与故障优先级测试；既有输出缺口、断流测试。 |
| Q-010：map 预分配 | 使用有界方案修复 | Endpoint 双向缓存、Client 发送成本与接收缓存按 `min(Window, 64)` 预留，默认窗口避免填满时连续扩容；任意大配置不导致巨大初始分配。实际流量仍受消息数量和字节预算控制。预分配会增加空闲会话的初始内存，属于明确取舍，不宣称总内存减少 40%。 | `TestPayloadCopyAndBoundedCapacity`，覆盖 0、1、64、MaxInt；实际满窗口与并发流验收。 |
| Q-011：Endpoint 字段混杂 | 已分组 | 生命周期、输入、输出、响应四个私有状态结构按值嵌入，统一由 Endpoint.mu 保护。没有引入每方向独立锁、指针对象或新包；不复制运行中的端点。 | 状态顺序、半关闭、幂等、终态、取消的全部既有回归与 race。 |

## 新补充的协议边界

- 窗口校验在已经确认 `Seq > consumed` 后使用 `Seq-consumed`，避免 `consumed+Window` 在 uint64 上限附近溢出。
- 两方向发送序号达到 MaxUint64 后返回 ResourceExhausted，禁止回绕到非法 DATA=0。
- 最大合法累计 ACK 使用“先递增再处理”的有界循环；最后一项、重复确认和较旧确认都不会回绕或重复扣减。
- DATA 控制结果的 `Consumed` 与订阅 ACK 使用相同的上界约束，拒绝未来确认之后取消会话，防止恶意确认触发长时间遍历。

## 为什么没有照搬部分示例

评审中的 `sync.Cond` 示例可能在等待 goroutine 真正调用 Wait 之前就完成 Broadcast，之后等待者永远睡眠；取消路径也可能等待一个尚未成功登记的等待者退出。示例还为每次等待创建通道与 goroutine，无法证明零分配或无泄漏。当前采用锁内登记的懒广播，直接兼容 context、多个等待者和客户端的输出缺口计时器，保留正确性且降低无等待者热路径成本。

“23 处 Unlock”是互斥分支中的静态代码位置，不是每条消息运行 23 次 Unlock。统一 defer 的收益在于锁边界清晰，不能据此宣称运行时锁操作减少 95%。race 检查验证数据竞争，不能用于测量锁竞争下降比例。

原报告的 100 msg/s、100μs ACK、总内存减少 40%、吞吐提升 30% 和锁竞争减少 50% 未附基线测量。本次以实际前后采样和完整功能验收报告为依据，不将这些数字当作已经达到的生产指标；也不以代码行数下降或两名人工评审的虚构确认作为门禁。

## 实测与验证

环境：Go 1.26.7、darwin/arm64、Apple M2 Max，GOMAXPROCS=12，无 race。每项五次采样，使用中位数；没有声称统计显著性。前测使用 0.1.9 实现加相同的基准，后测使用冻结后的 0.1.10 实现。

| 本地路径 | 前测 ns/op | 后测 ns/op | 前/后 B/op | 前/后 allocs/op |
|---|---:|---:|---:|---:|
| EndpointACK/64 | 610.3 | 49.89 | 112/0 | 1/0 |
| EndpointACK/1024 | 7990 | 57.62 | 112/0 | 1/0 |
| EndpointNotify/false | 34.6 | 8.084 | 112/0 | 1/0 |
| EndpointNotify/true | 36.73 | 39.21 | 112/112 | 1/1 |
| SessionDecode/string | 440.5 | 454.3 | 272/272 | 6/6 |
| SessionDecode/raw | 663.6 | 428.1 | 272/224 | 6/5 |
| SessionDecode/object | 753.1 | 747.2 | 416/416 | 11/11 |

窗口 64 的持续单条累计 ACK 热路径耗时下降约 91.8%，窗口 1024 下降约 99.3%；这些测量同时包含窗口补充与真实控制入口，排除初始化、引擎调度和远程传输。没有把本地 ns/op 解释为任务 ACK 往返延迟。

有等待者的通知中位数反而略慢约 6.8%，仍为一次通道分配；字符串解码中位数略慢约 3.1%，分配不变，普通对象基本一致。懒通知主要改善无人等待的更新；JSON 快路径的明确收益来自 RawMessage，不能宣称所有输入类型都更快。

| 同一后测二进制中的复制对照 | ns/op 中位数 | B/op | allocs/op |
|---|---:|---:|---:|
| append/253 | 40.43 | 256 | 1 |
| exact/253 | 36.93 | 256 | 1 |
| append/4097 | 483.5 | 4864 | 1 |
| exact/4097 | 489.7 | 4864 | 1 |

复制结果验证了切片容量更精确，但相同长度的两种复制占用同一分配器大小类别：253 字节均分配 256 B，4097 字节均分配 4864 B。保留复制是为了消息独立所有权，不声称减少分配次数或总堆字节；未对其他模块批量替换。

原始采样：[前测](benchmarks/0.1.10-session-before.txt)、[后测](benchmarks/0.1.10-session-after.txt)；全部样本、命令和比较算法数据见 [optimization-benchmarks.json](optimization-benchmarks.json)。

完整功能验收已通过；前序失败不删除，调查和复验过程如下。前两轮完整引擎测试中，middleware 最后一次取消流握手都超时；隔离重跑在 35.89 秒内通过全部断言，不能据此认定已经解决。

数据库只读调查确认失败 RUN `1a92bdec-8f97-4afd-9bb8-335c3e8457ec` 从 QUEUED / REQUEUED_NO_WORKER 直到 SCHEDULING_TIMED_OUT 都未分派 Worker；同期控制 PING 持续完成，业务与控制 Worker 持续心跳。更早的 START 有约 16 秒、29 秒分派延迟。这些事实将故障定位到 handler 执行之前的调度等待，但不单凭它们断言具体服务端缓存缺陷。

确认无运行任务和在线 Worker 后，重启现有本机开发引擎清除内存调度状态，保留镜像 v0.107.0、数据库与 PostgreSQL MQ。没有修改 Hatchet 源码、调度表数据、SDK 30 秒测试握手预算或断言；重启后的 middleware 在 15.24 秒内通过全部断言；它同时暴露事件示例在 handler 通知后立即读异步 OLAP 的 404 时序问题。已补充受原场景预算约束的只读可见性等待，仅重试 404，不重发业务任务，不吞掉 500、空成功响应或取消。`TestEventRunVisibility` 覆盖成功复制、服务错误、提前取消和预算到期；随后重跑全部必需验收。历史失败和调查数据见 [运行诊断](deep-quality-runtime-diagnostics.json)。

## 最终验收与证据

完成时间：`2026-10-07T15:14:27.098397+00:00`。SDK 0.1.10，协议 3，官方发行依赖 v0.109.0，本机引擎 v0.107.0，PostgreSQL MQ。

- 15 项门禁全部通过：源码边界、发行依赖、格式、单测、vet、race、上游及 embedded 上游回归、SDK embedded 模块、可复现生成、协议 fuzz、Compose、真实引擎、真实引擎 race、独立 embedded。
- 28 个源文件 / 74 个 standalone 片段、30 个示例场景 / 252 条必需断言全部通过；三种流、9 组故障、batch 关闭、durable 重启与 pending memo 两次恢复均通过。
- 新增 11 个协议边界测试；事件可见性额外覆盖四种结果；会话包 race 重复运行 10 次通过。
- 测试定义、触发资源、临时监听与连接已清理；embedded 数据库已删除。运行历史及引擎 Worker 记录按验收规则保留。
- 托管 CI 未执行；SDK CI 模板与 Compose 启动配置本机验证通过，使用 PostgreSQL MQ 和 SERVER_SECURITY_CHECK_ENABLED=false。

最终源文件：215 个 Go 文件。摘要 `3dda6483d707a476448a76f9eb13575eb1af4e720ad8b13a852d0d5bdad998fa`；算法为排序后的 SDK 相对路径、NUL、源码字节、NUL（忽略隐藏目录）。

复核入口：[机器可读完成报告](deep-quality-fixes.json)、[完整功能验收](design/acceptance-report.md)、[逐片段命令与 RunID/WorkerID/traceID](design/acceptance-report.json)、[全部基准样本](optimization-benchmarks.json)。0.1.9 的完整验收另存 [历史报告](design/acceptance-report-0.1.9.json)，不改写历史版本。

本轮真实串行与四会话并发的成本样本也保存在完成 JSON 的 execution_evidence 中。样本包含握手与排队，不是持续吞吐压测；统计提交任务数不包含输出发布与辅助查询。不得用局部 ACK 微基准替代这些引擎指标。

前两次调度超时和一次事件读取 404 都保留诊断记录。空闲引擎重启后完成完整验收，不等于已经修复或证明 v0.107.0 的长期调度状态稳定；本次没有改动服务端实现，生产规模、长期负载与跨机器验证仍不在本轮证据范围内。

## Claude 复审建议

请重点复核累计 ACK 上界、最大序号、广播登记与故障消费的线性化关系，以及控制任务发布与会话锁的边界。对照 `internal/session/state.go`、`control.go`、`endpoint.go`、`client_protocol.go`、`client_stream.go` 和 `optimization_test.go`；公开 API 与协议字段未变。保留对其他未触及模块的独立判断，不能把本报告当作整个 SDK 已不存在其他问题的证明。
