# RPC 流与历史读取设计评审

评审对象：[nats-rpc-stream-design.md](nats-rpc-stream-design.md)（2026-10-15 版，目标协议 7）。评审日期：2026-10-15。

## 0 前提与依据

- 按要求**不考虑历史兼容和迁移**：凡是只为旧协议、旧 token、旧配置、旧资源共存而存在的内容，一律按“可删除”处理。
- 结论对照了当前代码与依赖源码，而不仅是文档自洽性：
  - Extra：`internal/services/extra/batch.go`（PublishBatch/AckBatch）、`consumers.go`（Fetch）、`streams.go`（GetMessage）、`server.go`（默认限额）、`TEST_REPORT.md`（部署边界）、`proto/v1/extra.proto`。
  - SDK：`runtime/stream.go`、`internal/spec/runtime.go`、`model/stream.go`、`internal/telemetry/protocol.go`、`examples/proto/demo.proto`。
  - NATS：`nats-server v2.15.1` 的 `server/stream.go`、`server/jetstream_batching.go`。
- 严重度：**高** = 实现前必须改，否则正确性错误或容量不可用；**中** = 明显的性能、泄漏或复杂度问题；**低** = 命名、表述、示例瑕疵。

## 1 结论摘要

方案的主干是对的：两个方向各自一条 subject、尾序号条件发布、durable consumer 保存消费进度、历史读取只读、明确不承诺恰好一次。主要问题集中在五处：

1. **BatchAck 丢失后的核对算法在当前 Extra 上做不到。** Extra 把 `batch_id` 哈希成 `sha256(物理前缀 + batch_id)` 写入 `Nats-Batch-Id`，SDK 拿不到物理前缀，无法复算；“确认前次批次已被服务端放弃”也没有任何 API。需要换成基于 `Nats-Msg-Id` 和 `GetMessage(sequence, subject)` 的 O(1) 核对（见 2.1）。
2. **容量模型缺失，按默认值算出的上限远低于预期。** 每次调用常驻 2 个 durable consumer 约 24 小时，加上 `ScopeMaxConsumers=10000`，单个 scope 每天大约只能承载 5000 次调用；Extra 长期流限额 128，每次调用两端各占一个常驻 Fetch，单个 Extra 实例只能撑约 60 个并发调用；`Replicas=3` 与 Extra 当前单节点部署直接冲突（见 2.2）。
3. **没有端到端背压。** 生产者只受共享 Stream 总容量约束。一个快生产者配一个慢消费者或已崩溃的消费者，会把整个租户的 RPC Stream 写满，`DiscardNew` 之后同租户所有调用都会失败（见 2.3）。
4. **历史读取的资源模型过重。** 每次 `Iterator`、`LastMessage`、`Contains` 都会创建一个 durable consumer，Bloom 要两个；示例 10.5 在“恢复去重”这一主场景下退化为 O(n²) 次全量扫描，外加 O(n) 次 consumer 创建（见 3.4、3.5）。
5. **可观测性完全没有设计。** 文中没有指标、日志字段、trace 传播、诊断工具或告警阈值；现有 `wego_stream_claim_conflict_total`、`wego_checkpoint_*` 等指标会随旧协议一起失效，也没有给出替代（见第 6 节）。

另外，在“不考虑迁移”的前提下，约四分之一篇幅（第 2 节替换范围表、P5、`ErrProtocolVersion`、资源名里的 `v7`、各处“迁移至”）可以直接删除；第 7 节列出了约 20 组同义词和不一致的命名。

## 2 高严重度问题

### 2.1 结果未知核对不可实现，且比需要的复杂（高 · 正确性）

位置：§4 第 3 段、§5.3、P0。

问题：

- §5.3 要求“按原生批次/消息标识与原始字节核对”。但 `batch.go` 第 119–124 行把客户端 `batch_id` 转成 `nativeID = sha256(scope.names + id)` 再写入 `Nats-Batch-Id`。`scope.names` 是租户和 namespace 的物理前缀，SDK 不可见，所以读回的 `Nats-Batch-Id` 无法与本地批次对应。
- “重建原子批次会话前，先确认前次已经提交或已经被服务端放弃”：NATS 只会在批次超时（默认 10 秒）后发出 abandoned advisory，Extra 没有暴露，SDK 无从确认。
- “独立有限扫描 + 原始字节比对”要建临时 consumer 并下载载荷。在冲突最频繁、系统最忙的时候，它反而是最贵的路径。
- Extra 的 `batchIDs` 防重是单实例内存 `sync.Map`；Extra 多副本时，同一 `batch_id` 的两次网络尝试仍可能并行。

依据：在 nats-server 2.15.1 中，批次消息在 `mset` 锁内按 `clseq` 递增连续分配序号，存储时保留原有 header（`stream.go` 约 7890–7930 行）；`Nats-Msg-Id` 由 Extra 原样写入（`batch.go` 第 55 行）。GetMessage 支持 `sequence + subject`，语义是“该 subject 上序号不小于 sequence 的第一条”（`streams.go` 第 167–178 行）。

建议改为：

1. 传输适配层给每条记录设置 `Nats-Msg-Id = <CallID>.<方向>.<批次号>.<批内序号>`。这是原生元数据，不进业务帧，符合 §4 的约束。
2. **每次网络尝试使用新的 `batch_id`**，业务批次身份由 Msg-Id 承载。未提交的旧会话没有 commit 记录，永远不会落盘；如果旧会话的 commit 仍在途，新旧两次尝试带着同一个 expected，最多只有一个能通过条件检查。这样就不再需要“确认被放弃”。
3. 结果未知时调用一次 `GetMessage(sequence = expected + 1, subject)`：
   - 不存在：未提交，用新 `batch_id` 重试；
   - Msg-Id 等于本批首条：已提交。批次是原子且连续的，最终序号 = 返回序号 + N − 1；
   - 其他：被其他写者抢先，本实例终止。
4. P0 用测试把“批次内序号连续”和“Msg-Id 在批次内去重的行为”固定下来，而不是依赖 ADR 的文字描述。

收益：删除“独立有限扫描”“原始字节比对”“确认放弃”三段逻辑，核对成本从 O(批次) 次读取降为 1 次 RPC。

### 2.2 容量与部署参数自相矛盾（高 · 资源）

| 约束 | 来源 | 推算结果 |
| --- | --- | --- |
| 每次调用 2 个正常 consumer，保留到恢复截止时间（约 24 小时） | §3.1 | — |
| ScopeMaxConsumers = 10000 | §9.3 | 单个 scope 每天约 5000 次调用，约 3.5 次/分钟；历史扫描 consumer 还要从同一预算中扣除 |
| Extra MaxConcurrentRPCs = 128（长期流单独计数） | `server.go` | Client 和 Worker 各持一个常驻 Fetch，再加上 PublishBatch，单个 Extra 实例约 60 个并发 RPC 调用 |
| Extra MaxPublishBatchesInFlight = 8，满时**立即**返回 ResourceExhausted | `batch.go` 第 70–75 行 | 整个 Extra 实例同时只能有 8 个批次在提交；设计没有把这种情况归为可重试 |
| commitBatch 逐条 `RequestMsgWithContext` | `batch.go` 第 141–167 行 | 100 条的批次需要 100 次到 NATS 的往返，并在整个期间占用一个名额 |
| StreamMaxBytes = 1 GiB（每个租户的 RPC namespace） | §9.3 | 与 Extra `MaxStoreBytes` 默认 1 GiB（全实例 JetStream 总预算）相等：一个租户的 RPC Stream 就能吃掉所有租户的存储，包括 KV 状态 bucket |
| Replicas = 3 | §9.3 | Extra 当前是单节点 embedded（`TEST_REPORT.md`“实现边界”），create-only 会直接失败；集群模式下每个 R3 consumer 都是一个 raft 组，1 万个 consumer 对 meta 层压力很大 |

建议：

- 在 §3.1 增加**容量模型**，用公式给出“并发调用数、日调用量、单调用最大字节数”与各限额的关系，再据此定默认值。
- 方向完成后尽早回收 consumer：本实例确认结束（EOF 已 ACK 且本地缓冲读空）后，经过一个短的宽限期即可删除；恢复时如果 consumer 不存在但尾部是 EOF，直接返回终态，不需要 consumer。这条与“不得删除共享 consumer 来通知其他实例”并不冲突，删除只发生在结束条件已经成立之后。
- 正常 consumer 显式设置 `Replicas=1`（或 memory storage）。它的进度丢失只会导致重复投递，不会丢数据。
- 基数为 1 的方向不建 consumer（见 3.6）。
- PublishBatch 名额耗尽和 Stream 容量耗尽必须能区分开（见 4.1），前者退避重试。
- Extra 侧建议把非 commit 消息改成不等待回复的发布（NATS 允许中间消息无回复，可每 N 条做一次流控），否则 §9.2 的 `BatchWait = 5ms` 在 Extra 内部的逐条往返面前没有意义。

### 2.3 没有端到端背压，单个调用可写满共享 Stream（高 · 资源）

位置：§3.1、§5.2。

问题：Send 只受本地 FIFO 约束，后台发布只受共享 Stream 的 `MaxBytes` 约束。Worker 没有启动（任务在 Hatchet 排队）、Worker 很慢或 Client 已经崩溃时，对端可以持续写入，直到整个租户的 RPC Stream 满。之后 `DiscardNew` 会拒绝该租户**所有**调用的写入，这正是 §3.1 描述的“容量满时拒绝写入”。gRPC 原生有 HTTP/2 流控窗口，这里完全没有。

建议：

- 每个方向设置硬上限：Stream 开启 `MaxMsgsPerSubject = N` 并配 `DiscardNewPerSubject = true`。这是拒绝写入，不是 §3.1 禁止的“历史淘汰”，正好作为单次调用的长度配额，超限返回 ResourceExhausted。
- 可选的软背压：生产者拿 `BatchAck` 的序号与对端 consumer 的 `AckFloor` 比较，差值超过 `Send.MaxLagMessages` 时暂停发布。代价是周期性查询 ConsumerInfo，可以复用结束检查的查询结果。
- 在 §1 的合同表里写明“生产速度不受对端消费速度约束，只受单调用配额约束”。

### 2.4 接收路径的 Fetch 语义没定义，可能产生秒级延迟和极小的 Pull（高 · 性能）

位置：§6.1。

问题：

- Fetch 是长轮询，`WaitTimeout = 5s`，最多一个在途。文中写“Pull 收到 DATA → 提交 ACK → 写入 chan”，但没有说明 ACK 和入 chan 是**每条到达就做**，还是**等整个 Fetch 结束再做**。如果是后者，低流量时一条消息最长要等 5 秒（Fetch 要么凑满条数，要么等到超时才结束）。
- `可请求条数 = min(BatchMessages, 容量 − 占用 − 在途预留)`。默认 `Recv.BufferMessages = Recv.BatchMessages = 100`：第一次拉满 100 条后 chan 就满了；业务每取走 1 条，下一次 Pull 只请求 1 条。这是典型的“糊涂窗口”问题，Fetch 次数与消息数成正比。
- 在途 Pull 的预留会一直占着 chan 空间直到超时，导致空闲流的缓冲长期被“预留”占满。

建议：

- 明确规定：消息按到达逐条校验，`AckBatchWait`（1ms）内攒成 AckBatch，ACK 返回后立即入 chan；在途 Fetch 不阻塞这条路径。
- 增加低水位：只有当可用空间不少于 `max(BatchMessages / 2, 1)`，或 chan 已经读空时，才发起新 Pull。
- 默认 `Recv.BufferMessages` 至少设为 `2 × BatchMessages`。

### 2.5 历史扫描依赖“临时具名 durable consumer”，泄漏和元数据开销都偏大（高 · 资源）

位置：§8.2、P1 表“历史临时 consumer”。

问题：

- Extra 只接受具名 durable，所以 SDK 要自己生成随机名、显式删除，并依赖 `InactiveThreshold` 兜底。一次 Bloom 要两个 consumer，每次 Contains 一个，每次 LastMessage 一个，每次 Iterator 一个。consumer 创建是 JetStream meta 层操作，集群下还要走 raft。
- `HistoryConsumerIdleTTL = 60s` 是使用陷阱：用户在 `for range history.Iterator()` 里处理单条超过 60 秒（例如调用外部服务），扫描就会报错。
- 删除失败只能等 InactiveThreshold 回收，泄漏的 consumer 仍计入 `ScopeMaxConsumers`，会挤占正常调用。

建议（二选一）：

- **首选**：Extra 新增一个只读 RPC，例如 `ReadSubject(stream, subject, start_seq, end_seq, max_messages, max_bytes) returns (stream Message)`。服务端用有序的临时 consumer 实现，不需要名字，RPC 结束即释放；SDK 侧就不存在命名、删除和空闲回收的问题，也不再消耗 consumer 预算。
- 次选：小历史直接用 `GetMessage(sequence, subject)` 逐条读取，零 consumer；只有超过阈值才建临时 consumer，同一句柄的多次操作复用同一个扫描结果或 consumer。

### 2.6 Bloom 加 Contains 的推荐用法在主场景下是 O(n²)（高 · 性能）

位置：§8.3、§10.5。

问题：示例 10.5 是 Worker 重试后“补发缺失输出”。这个场景下大部分 key 已经存在于历史中，Bloom 对它们都返回 true，于是**每个已存在的 key** 都要做一次 `Contains` 全量扫描（并新建一个 consumer）。n 条历史、n 个候选，结果是 n 次全扫描。Bloom 自身还要扫两遍。概率结构只在“绝大多数查询为否”时才有收益，而这里恰好相反。

建议：

- 首版删除 Bloom、BloomOption、Contains，改为一次扫描构建精确集合，例如：

```go
func Keys[T proto.Message](h StreamHistory, key func(T) string) (map[string]struct{}, error)
```

  用 `History.MaxCollectBytes` 限制内存，超限返回 ResourceExhausted，提示业务改用自己的持久索引。
- 如果仍要保留 Bloom：用临时 consumer 创建时的 `NumPending` 作为条数上界，一遍完成，不需要“先统计”的那一遍；Contains 必须复用同一份已扫描的结果，不能每次重扫。
- 用 `func(T) string` 这类类型化提取函数代替字段路径字符串，§8.3 中关于 descriptor 解析、类型标记、int64 的 ProtoJSON 字符串化、拒绝 repeated/map/浮点等规则可以整段删除，`MayContain` 也不会再因为 `any` 类型不匹配而需要返回 error。

### 2.7 Client 崩溃后 Worker 无法感知，slot 被占到任务超时（高 · 泄漏）

位置：§3.2“暂时没有消息不表示输入结束”、§7.2。

问题：client 方向没有 EOF 时，Worker 的 Recv 会一直等待。Client 崩溃时不会请求 CancelRun（只有存活的创建者才会取消），而 `WithResumeSend` 又允许之后恢复发送，所以 Worker 不能自行判定结束。结果是 Worker 的 slot、Fetch 长期流、内存缓冲都被占用到 Hatchet 任务执行超时；流式任务通常会把超时设得很长。

建议：

- 增加输入空闲期限：client 方向超过 `Recv.IdleTimeout`（方法级可配，默认例如 5 分钟）没有新记录，也没有 EOF，Worker 的 Recv 返回 DeadlineExceeded，handler 按普通错误处理。
- 或者，Client 在 Send 方向空闲时周期性刷新调用描述中的 `LastSeenAt`，Worker 的结束检查顺带读取这个字段。这只是活性信号，不涉及 reader 所有权，与 §2 删除 CLAIM 的目标并不冲突。
- 恢复截止时间本身也必须让 Worker 的 Recv 结束，§6.2 目前只说“历史过期不能当成功 EOF”，没说要不要返回错误。

## 3 中严重度问题

### 3.1 DATA 使用 ProtoJSON（中 · 性能）

位置：§4。ProtoJSON 的编解码通常比二进制 protobuf 慢数倍，体积更大（bytes 字段走 base64，int64 编成字符串），还直接造成 §8.3 的“大整数混淆”规则。既然不考虑兼容，DATA 载荷应改为二进制 protobuf wire format；类型由调用描述给出，帧类型放在 NATS header（例如 `Wego-Frame: data|headers|eof`），载荷本身不再套 oneof 信封，免去一次拷贝。如果确实需要人工可读（控制台调试），由诊断工具按 descriptor 转成 JSON 显示。

### 3.2 大小预算互相冲突（中 · 正确性）

- `MaxMessageBytes = 1 MiB`（解码后）等于 `MaxEncodedFrameBytes = 1 MiB`（含控制信息），而 ProtoJSON 会让体积膨胀，接近上限的合法消息必然编码超限。需要写明 `MaxEncodedFrameBytes > MaxMessageBytes` 的比例，或说明超过某个阈值后走对象存储 codec。
- `Send.BatchBytes = 4 MiB` 等于 Extra 的 `MaxPublishBatchBytes = 4 MiB`，但 Extra 按 `m.Size() + 256` 累计（`batch.go` 第 109 行）。一个刚好 4 MiB 的批次会被拒绝，并且返回的是 ResourceExhausted，SDK 会误判为可重试而反复重试。SDK 必须用同一公式预算，或默认值留出余量。
- Extra 的 Fetch 也按 `len(m.Data) > MaxMessageBytes` 拒绝（`consumers.go` 第 317 行），这是**编码后**的大小，与 SDK 的“解码后”含义不同，文档应统一用“编码后帧大小”和“解码后消息大小”两个名字。
- §9.3 说“启动时校验服务能力和部署预算”，但 Extra 的 26 个 RPC 中没有能力或限额查询接口。要么 Extra 增加 `GetLimits`，要么在文档里写死与 Extra 版本绑定的常量。

### 3.3 发送组批等待增加交互延迟（中 · 性能）

`Send.BatchWait = 5ms` 让每条交互式 bidi 消息至少多等 5ms。由于每个方向本来就最多只有一个批次在途，更好的规则是：**没有批次在途时立即发送；有批次在途时累积，前一批确认后立刻发出累积的全部**。吞吐不变，空闲时延迟降到一次往返，也不需要定时器。可以保留 `BatchWait` 作为可选的额外聚合，默认为 0。

### 3.4 结束检查的轮询成本（中 · 性能）

位置：§6.2、§9.2。每个实例的每个方向每秒做一次“读尾部 + GetConsumer”。1000 个并发调用、两端各两个方向，就是每秒 4000 次 Extra RPC，其中 ConsumerInfo 在集群下还要访问 consumer leader。

建议：

- 正常路径由事件触发：本实例收到 EOF 且 EOF 已 ACK 后检查一次，不满足就退避重查（例如 100ms 起，指数增长到 5s）。
- 没收到 EOF 的实例（新旧实例并存时才会出现）只在 Fetch 空返回时检查，同样指数退避，上限例如 30s。Delivery 自带的 `metadata.num_pending` 可以作为免费的提示信号。
- 结束条件可以化简为“尾部是 EOF(S) 且 `AckFloor.Stream ≥ S` 且本地已读空”。S 是该 subject 的最后一条，ACK floor 越过 S 就已经蕴含 `NumPending == 0` 与 `NumAckPending == 0`，后两个条件是冗余的。

### 3.5 AckWait 60 秒没有依据（中 · 恢复延迟）

ACK 已经前移到“SDK 收到即确认”，与业务处理时长无关，`AckWait` 只需覆盖“收到到 ACK 往返”的时间，60 秒过长。实例崩溃时，它已拉取但未 ACK 的消息要等 60 秒才会重投，结束检查也会被拖住 60 秒。建议 10–15 秒，并写明计算依据。

### 3.6 基数为 1 的方向不需要 consumer，两个“不可用”错误可以删除（中 · 复杂度）

位置：§7。

- Server streaming 的请求方向只有“1 条 DATA + EOF”。Worker 用 `GetMessage(sequence = 1, subject = rpc_xxx_client)` 就能 O(1) 读到这条请求，重试时也一样能读到，`ErrRequestUnavailable` 不会出现，这个方向也不需要 `_client_rx` consumer。
- Client streaming 的响应同理：等待阶段可以继续用 consumer，但读不到时应该直接从 server subject 历史读取这唯一一条，而不是返回 `ErrResponseUnavailable`。文中“不从发布历史中自动补交”没有给出理由，而补交只是一次 RPC。
- 收益：每次 server/client streaming 调用少一个 consumer、少一个 Fetch 长期流，少两个错误类型以及它们在示例和验收中的分支。

### 3.7 Worker 重试时 Recv 会悄悄从中间开始（中 · 错误处理）

位置：§7.2。重试的 attempt 绑定同一个 `_client_rx` consumer，已经 ACK 的输入不会重投，handler 的 Recv 直接从中间开始，自己却不知道。文中说“业务需要自行处理”，但没有给出任何信号。

建议二选一，并写进 §7.2：

- client/bidi 方法默认禁止 Hatchet 重试，注册时如果配置了重试就拒绝，除非显式声明 `WithStreamRetry`；
- 或者在 `attempt > 0` 时，SDK 让 Recv 先返回一个可识别的错误（例如 `ErrStreamResumed`），或在 task Context 中暴露 `task.StreamResumed(ctx)`，强制 handler 走历史恢复路径。

### 3.8 资源回收职责没有归属（中 · 泄漏）

文中出现了四种回收：正常 consumer 的“独立 GC”（§3.1）、初始化失败和提交失败的“有界清理任务”（§3.2）、历史 consumer 的“显式删除 + 空闲回收”（§8.2），以及对象存储 codec 的内容和解码密钥（§3.1 只要求“覆盖恢复期限”，没说谁删）。前三者没有说明由谁运行（Client、Worker、Extra 还是一个 Hatchet 定时任务）、多实例如何避免重复执行、失败后如何重试；对象存储的对象完全没有回收描述。

建议：

- 正常 consumer 设置 `InactiveThreshold`（取恢复窗口长度），由 NATS 服务端回收，再加上 2.2 的“完成即删除”，删除独立 GC。
- 初始化孤儿问题用 3.9 的按需创建消除。
- 对象存储用 bucket 生命周期规则（过期时间 = MessageTTL + 安全余量）回收，在 §9.3 写明。

### 3.9 Initializing/Ready 状态机可以删除（中 · 过度设计）

位置：§3.2。Stream 用的是 LimitsPolicy，durable consumer 用 `DeliverAll` 加精确 filter，**创建时间不影响能看到的消息**，所以“两个 consumer 都建好才允许发布和提交任务”的门禁没有必要。各端在首次接收时用 create-only 创建自己的 consumer，返回 AlreadyExists 就读回校验配置，从而去掉 Initializing/Ready 状态、补建逻辑和孤儿清理任务。调用描述只需要一次 KV Create 写入身份，RunID 绑定用一次 CAS 回填。

同时：§3.2 的“稳定提交身份；结果未知时查找绑定”没有说明用 Hatchet 的什么机制实现。现有 `WithStreamIdempotencyTTL` 会怎样处理也没写，需要补上。

### 3.10 历史句柄把 Context 存起来，并且有两套并发限制（中 · API）

- §8.1“方法使用创建句柄时绑定的 Context”违背 Go 的惯例，单个操作无法设置独立超时，`History.ScanTimeout` 正是为弥补这一点才出现的。建议所有读取方法接收 `ctx`，删除 `ScanTimeout`。
- `ErrHistoryBusy`（每个句柄同时只允许一个扫描）与 `MaxConcurrentScans`（每实例 4 个）重复。既然每次 Iterator 本来就是独立扫描，前者没有必要。
- 句柄创建时只读了尾部，不持有任何资源，扫描的清理也由迭代器的 defer 负责，`Close` 实际上没有职责。可以删除 Close，或者明确它只用于取消在途扫描。

### 3.11 StreamRef 和 ResumeStream 的可用性（中 · API）

- 业务要把 StreamRef 存起来，跨进程后再恢复，但文中没有给出序列化接口。需要 `String()`/`ParseStreamRef` 或 `MarshalText`/`UnmarshalText`，并固定编码格式。
- `conn.ResumeStream` 返回无类型的 `grpc.ClientStream`，业务得自己包装成生成桩的类型。建议提供泛型版本，例如 `client.ResumeBidi[Req, Resp](ctx, conn, ref)`，或者让生成桩的方法接受一个 `client.WithResume(ref)` 调用选项，原有调用方式保持不变。
- 鉴权粒度：调用的历史读取和 `WithResumeSend` 只校验租户。同租户内任何拿到 CallID 的客户端都能读取全部请求和响应，甚至注入输入。至少要在 §3.2 写明这一边界；如果需要更细的权限，在调用描述中记录创建者身份。

## 4 错误处理路径

### 4.1 缺少统一的错误分类表

文中有约 15 处错误描述分散在各节（“暂时性失败”“终止错误”“返回相应错误”“明确的不确定/传输错误”等），但没有一张“来源 → 分类 → 动作 → 对业务暴露的 gRPC code”的表。具体缺口：

- Extra 对“批次名额耗尽”（暂时性）、“批次超过条数/字节上限”（配置错误，永久）和 NATS 存储满（`DiscardNew`）都返回 ResourceExhausted，SDK 仅凭 code 无法区分。要求 Extra 在 status details 中加上 `ErrorInfo.reason`，文档列出 reason 枚举。
- `FailedPrecondition`（尾序号冲突）在一次重试之后既可能是“自己的前次提交已成功”，也可能是“其他写者抢先”，统一走 2.1 的核对流程。
- `Aborted`（同 batch_id 已在途）在 2.1 的方案下不会再出现；保留现有方案的话也必须分类。
- 后续 Send 返回的“终止错误”与触发它的批次无关，需要包装出原因（code、方向、expected、失败时间），否则业务看到的只是一个莫名其妙的 Send 失败。

建议在 §5 或附录增加一张不超过 15 行的错误表，覆盖：可重试、需核对、终止当前实例、终止整个调用。

### 4.2 终态来自两个来源，但没有判定表

Client 的最终结果取决于“是否存在有效 EOF”与“Hatchet 运行状态”两个来源，§7.1 分了五种情况，§7.1 末尾和 §7.2 又各补了几句。建议给出一张判定表，例如：

| server 尾部 | 运行状态 | Client 结果 |
| --- | --- | --- |
| EOF(OK) | 任意 | 读空后返回 io.EOF |
| EOF(status) | 任意 | 读空后返回该 status |
| 无 EOF | 运行中或重试中 | 继续等待 |
| 无 EOF | 最终失败 | 读空后返回 Unknown 或 Internal，并带上运行错误 |
| 无 EOF | 已取消 | 读空后返回 Canceled |
| 无 EOF | 恢复期限已过 | 返回 DeadlineExceeded 或专用的过期错误 |

还要补充“运行观察器”的实现方式：订阅 Hatchet 的运行事件还是轮询？每个调用一个还是进程内共享？它的成本应计入 2.2 的容量模型。

### 4.3 “业务错误是否重试”的判定规则缺失

§7.1 区分了“明确不重试的业务错误（写 EOF(status)）”和“需要 Hatchet 重试的失败（不写 EOF）”，但没有说明 SDK 如何判断：是看 gRPC status code、`model` 包中的某个 NonRetryable 包装，还是看方法的重试配置。同时，最后一次 attempt 失败时，由于“不能单凭 RetryCount 推测最终失败”，不会写 EOF，客户端只能依赖运行观察器。这条路径应在 4.2 的判定表中覆盖，并在 §12 增加验收用例。

### 4.4 其他

- §5.4 写“对端已经结束响应时，新的 Send 可返回对端已关闭错误”，这里的“可”是可选还是必须？应该写成确定的行为，并给出错误值。
- §7.2 的 Context 取消语义是：创建调用的客户端取消 ctx 时会同时取消 Run。但示例中普遍使用 `defer cancel()`，正常结束时也会触发。需要写明“终态已确定后的取消不会触发 CancelRun”。
- §6.2“恢复期限内禁止自动回收正常 consumer”与 2.2 的“完成后删除”需要一起改写。

## 5 性能验收标准不可执行

§12 末段只要求“记录”延迟、吞吐等指标，没有目标值，也没有负载模型，因此无法判定通过还是失败。建议至少给出：

- 负载模型：消息大小（1 KiB、64 KiB、1 MiB），并发调用数（1、100、1000），三种 RPC 形态；
- 目标：单条消息端到端 P50/P99 延迟（空闲 bidi），单方向持续吞吐，每个调用的常驻 goroutine 数与内存上限，每个调用的 Extra 长期流数和 consumer 数，历史扫描速率；
- 对照组：协议 6 当前实现的同场景数据，只作为基线参考，不要求兼容。

## 6 可观测性设计缺失（高）

全文没有可观测性章节。现有 `internal/telemetry/protocol.go` 中注册的 `wego_stream_claim_conflict_total`、`wego_checkpoint_restore_total`、`wego_checkpoint_history_messages`、`wego_output_prefetch_*` 会随旧协议一起失去意义，必须给出替代。建议新增一节，至少包含以下内容。

### 6.1 指标

沿用 `wego_` 前缀。标签只使用低基数维度：`method`、`direction`（request/response）、`role`（client/worker）、`outcome`。**不得**把 CallID 或 RunID 作为标签。

| 指标 | 类型 | 用途 |
| --- | --- | --- |
| `wego_rpc_stream_calls_active` | gauge | 当前活跃调用数，与容量模型对照 |
| `wego_rpc_stream_send_queue_messages` / `_bytes` | gauge | 本地 FIFO 占用，判断 Send 阻塞的原因 |
| `wego_rpc_stream_publish_batch_size` | histogram | 实际批次大小，验证组批是否生效 |
| `wego_rpc_stream_publish_duration_seconds` | histogram | PublishBatch 往返耗时 |
| `wego_rpc_stream_publish_retries_total{reason}` | counter | 重试原因：名额耗尽、超时、不可用 |
| `wego_rpc_stream_publish_unknown_total{resolution}` | counter | 结果未知的核对结论：已提交、未提交、竞争失败、无法核对 |
| `wego_rpc_stream_publish_conflicts_total` | counter | 条件竞争失败，取代 claim_conflict |
| `wego_rpc_stream_recv_buffer_messages` / `_bytes` | gauge | 接收 chan 占用 |
| `wego_rpc_stream_fetch_messages` | histogram | 每次 Fetch 实际拿到的条数，用来发现 2.4 的小 Pull |
| `wego_rpc_stream_redeliveries_total` | counter | `num_delivered > 1` 的投递次数，是重复交付的直接来源 |
| `wego_rpc_stream_ack_failures_total` | counter | ACK 失败次数 |
| `wego_rpc_stream_completion_checks_total{result}` | counter | 结束检查次数和结果，验证轮询成本 |
| `wego_rpc_stream_completion_latency_seconds` | histogram | 从 EOF 提交到 Recv 返回 io.EOF 的时间 |
| `wego_rpc_stream_terminal_total{source,code}` | counter | 终态来源（EOF 或运行状态）和状态码 |
| `wego_rpc_stream_history_scans_total{op,outcome}` / `_scan_messages` | counter / histogram | 历史读取次数与规模 |
| `wego_rpc_stream_consumers_deleted_total{kind,outcome}` | counter | consumer 回收结果，删除失败就意味着泄漏 |

### 6.2 日志

使用 `l` 结构化字段：`call_id`、`direction`、`role`、`run_id`、`task_run_id`、`attempt`、`expected`、`batch_id`、`stream_seq`。要求：

- 每个调用只在建立、恢复、终态这几个节点打 info；
- 竞争失败、结果未知、核对结论、终止错误打 warn 或 error，带上 `.Err(err)`；
- 不在每条消息上打日志（规则：避免逐行记录状态）。

### 6.3 Trace

- Client 的调用 span 作为根，Worker 的 handler span 通过 Hatchet 运行元数据链接到它（trace context 写入调用描述或任务输入，**不**写入每条消息）。
- 子 span：PublishBatch、结果核对、Fetch（只在耗时超过阈值时记录，避免空闲轮询产生大量 span）、结束检查、历史扫描。

### 6.4 诊断与告警

- 诊断工具：给定 StreamRef 或 CallID，输出调用描述、两个方向的尾部（类型、序号、EOF 状态）、两个 consumer 的 `AckFloor`/`NumPending`/`NumAckPending`/`NumWaiting`、绑定的运行状态。可以做成 hatchet CLI 的子命令或 Extra 的管理接口，否则线上排查“为什么 Recv 不结束”只能手工调用多个 RPC。
- 并发读者检测：`NumWaiting ≥ 2` 说明新旧实例在分抢同一个 consumer（§6 允许，但会让业务看到不完整的流），应打 warn 日志并计数。
- 告警阈值：RPC Stream 字节使用率（例如 80%）、consumer 数占 `ScopeMaxConsumers` 的比例、历史 consumer 存量（应接近 0）、结果未知比例、重投比例。

## 7 过度设计与可删除内容

在“不考虑兼容和迁移”的前提下：

| 内容 | 位置 | 处理 |
| --- | --- | --- |
| 替换范围表（13 行“旧 → 新”） | §2 | 删除，改为一段“范围与非目标” |
| P5 迁移与发布、协议 6/7 并行路由、旧运行排空 | §11 | 删除，只保留发布门禁那一段 |
| `ErrProtocolVersion`、拒绝旧 token | §3.2 | 删除 |
| 资源名中的协议号 `rpcv7`、`wego_rpc_v7`、`wego_rpc_state_v7` | §3.1、§9.3 | 改为 `rpc`、`wego_rpc`、`wego_rpc_state`；协议版本只放在调用描述里 |
| “旧 Window/BufferBytes 迁移至…” | §2、§9.2 | 删除，直接给出新配置 |
| 两个仅剩跳转的协议文件 | `nats-stream-*-protocol.md` | 直接删除；不删的话至少修正标题（见第 10 节） |
| Initializing/Ready 状态与孤儿清理 | §3.2 | 见 3.9 |
| 独立 GC 任务 | §3.1 | 见 3.8 |
| DescriptorTTL（48h）与 MessageTTL（24h）分离 | §3.1 | 合并为一个保留期。描述只多活 24 小时，用途仅仅是把 NotFound 换成“已过期”，NotFound 统一映射成过期错误即可 |
| Bloom、BloomOption、Contains、两遍扫描、类型规范化规则 | §8.3 | 见 2.6 |
| `ErrHistoryBusy`、句柄 `Close` | §8.2 | 见 3.10 |
| `ErrRequestUnavailable`、`ErrResponseUnavailable` | §7 | 见 3.6 |
| 结束条件中冗余的 NumPending/NumAckPending | §6.2 | 见 3.4 |
| 暴露给业务的 `CompletionCheckInterval/Timeout`、`Recv.WaitTimeout` | §9.2 | 移到 §9.3 的内部参数，与 §9.1“组件选择留在内部”的原则保持一致 |

历史 API 的建议形态（首版）：

```go
type StreamHistory interface {
	Snapshot() HistorySnapshot
	All(ctx context.Context) iter.Seq2[*StreamMessage, error]
	First(ctx context.Context) (*StreamMessage, error)
	Last(ctx context.Context) (*StreamMessage, error)
}

func Collect(ctx context.Context, h StreamHistory) ([]*StreamMessage, error)
func Keys[T proto.Message](ctx context.Context, h StreamHistory, key func(T) string) (map[string]struct{}, error)
```

`Last` 目前是 O(n)。如果要做到 O(1)：尾部不是 EOF 时，尾部就是最后一条 DATA，用 `GetMessage(last_subject)` 一次读出；尾部是 EOF 时，让 CloseSend 把 EOF 作为单独一批提交，EOF 中写入它的 expected（即最后一条 DATA 或 HEADERS 的序号），代价只是关闭时多一次往返。如果不想要这个约束，就把 Last 从首版删除。

另外，HistorySnapshot 有 `Sealed` 字段，却不提供 EOF 携带的最终状态。建议增加 `Status *status.Status`（只在已封口时非空）。

## 8 重复内容

同一约束在多处重复出现，修改时容易漏改：

- “不伪造或补写成功 EOF”出现在 §5.4、§6.2、§7.1（两处）、§7.2、§12；
- “业务自行负责幂等和副作用，不是恰好一次”出现在 §1、§5.2、§7.2、§12 末段；
- “不增加 writer_id/record_seq/累计计数”出现在 §4、§5.3、§8.3、P0、P4；
- “Send 成功消息可能仍在本地”出现在 §1 合同表、§8.2、§10.2 段尾、§10.5 段尾；
- §8.2 的快照与懒加载规则在 P4 中几乎逐句重复；
- §9.1 与 §10.1 末段重复说明“不把原生字段作为 SDK 配置名”。

建议：§1 的合同表加一张“非目标”表作为唯一出处，其他位置只引用；§11 的各阶段只列交付物和验收项，不再复述规则。

另外，§11 P1 写着“已实现并部署”，而文首状态是“目标设计，尚未实现”。实施状态会随时间变化，应移到实施记录（`TEST_REPORT.md` 或单独的进度文档），设计文档只保留能力要求。

## 9 命名一致性

| 概念 | 文中的不同说法 | 建议 |
| --- | --- | --- |
| 持久保存的调用信息 | 调用描述、持久描述、描述 | 统一为“调用描述”，Go 类型 `CallDescriptor` |
| 恢复用的引用 | 调用引用、恢复引用、StreamRef、调用身份 | 统一为 StreamRef；“调用身份”专指 CallID |
| 正常接收用的 consumer | 正常 consumer、正式 consumer、稳定 consumer、具名 durable | 统一为“方向 consumer” |
| 历史扫描用的 consumer | 临时历史 consumer、临时具名 consumer、临时 Pull consumer | 统一为“扫描 consumer”；采纳 2.5 后这个概念消失 |
| 方向结束 | EOF、封口、最终封口、发送关闭、半关闭、Sealed | 记录名用 EOF；状态名用“已封口/Sealed”；“半关闭”只指 CloseSend 这个动作 |
| 写入方 | 生产者、写者、writer、发布者、发布方 | 统一为“生产者” |
| “实例” | SDK 进程、一次 attempt 的执行、Extra 服务实例混用 | 分为“SDK 实例”“执行（attempt）”“Extra 实例” |
| 服务名 | Extra、Hatchet Extra、流服务、hatchet-extra | 正文统一用 Extra，首次出现时注明 `hatchet-extra` |
| 期限 | 恢复截止时间、恢复期限、生命周期、可验证生命周期、保留期限 | 只保留“保留期”（数据）和“恢复截止时间”（调用）两个 |
| ACK 状态 | ACK 提交成功、ACK 生效、ACK 已成功 | Extra 的 `confirm=false` 只表示已发往 NATS，所以应统一为“ACK 已发出”，不使用“生效” |
| 历史入口 | `task.ClientStream` / `task.ServerStream` | 与 §7 中的 RPC 形态 Client streaming / Server streaming、以及 `grpc.ClientStream` 接口同名，含义却是“方向”。在 server streaming RPC 里调用 `task.ClientStream` 读请求，很容易误读。建议改为 `RequestHistory` / `ResponseHistory`；Client 侧也要用，放在 `task` 包也不贴切，建议放到 `stream` 包或同时在 `client` 包导出 |
| subject 与 consumer 名 | `rpc_xxx_client`、`rpc_xxx_client_rx` | “client”指生产方，“rx”指消费方，`rpc_xxx_client_rx` 很容易被读成“client 接收”。建议 subject 改为点分形式 `rpc.<id>.req`、`rpc.<id>.resp`，这样 Stream 可以订阅 `rpc.>`，不必占用整个 namespace 的 `>`（§3.1 中“禁止另建重叠 Stream”的限制也随之消失）；consumer 名为 `rpc_<id>_req`、`rpc_<id>_resp` |
| ID 长度 | “32 位十六进制 UUID” | 改为“32 个十六进制字符（128 位）” |
| 历史方法 | `AllMessage`、`Iterator` | 语法上应为 `AllMessages`；按 Go 1.23 惯例，返回 `iter.Seq2` 的方法叫 `All` |
| 时间参数 | `BatchWait`（攒批时长）、`WaitTimeout`（Fetch 等待）、`MinRetryWait`、`WriteTimeout`、`ScanTimeout`、`CompletionCheckTimeout` | “Wait”同时表示攒批和超时。建议攒批用 `Linger`，超时统一用 `Timeout`，退避用 `RetryBackoffMin/Max` |
| 保留期参数 | `MessageTTL` | 与 NATS 的单条消息 TTL（`Nats-TTL`，Extra 批次明确拒绝 `message_ttl_ns`）混淆，应改为 `Retention` 或 `MaxAge` |
| 协议版本 | 协议 7、RPC 流协议 7 | 统一为“RPC 流协议 7” |
| 错误名 | `ErrStreamContext` | 无法从名字看出含义，建议 `ErrNoStreamInContext` |

## 10 配置审查

### 10.1 公开配置（§9.2）

- **项目过多。** 业务可见的有 26 项，其中约一半是内部机制（结束检查、Fetch 等待、扫描批次）。建议公开配置只保留：`Address`、`TLSConfig`、`MaxMessageBytes`、`Send.BufferMessages/BufferBytes`、`Recv.BufferMessages/BufferBytes`、`Recv.IdleTimeout`（见 2.7）、`History.MaxScanMessages/MaxScanBytes/MaxCollectBytes`、`History.MaxConcurrentScans`，其余移到内部参数。
- **`Send.WriteTimeout = 10s`。** Extra 的 commitBatch 自带 30 秒预算，NATS 的批次超时是 10 秒。客户端 10 秒超时后，服务端可能仍在提交，结果未知的情况会因此频繁出现。应写明三者的大小关系（客户端超时应大于服务端单批预算），或者让 Extra 以客户端 deadline 为准（gRPC deadline 已经会传到 `out.Context()`，需要在文档里说明）。
- **`Recv.BufferMessages = Recv.BatchMessages = 100`**：见 2.4。
- **`History.BloomMaxBytes = 16 MiB` 永远不会生效。** `MaxScanMessages = 100 万`、p = 0.0001 时，位图约 2.4 MB。
- **`History.ScanTimeout`**：改为由 ctx 控制后删除（见 3.10）。
- **`History.AllMessageMaxMessages/AllMessageMaxBytes`**：随 API 改名为 `MaxCollectMessages/MaxCollectBytes`，并可合并为只保留字节上限。
- **方法级 `StreamOptions` 的字段列表没有给出。** 只说“覆盖 Send、Recv、History 及消息大小”，没有说明是整体替换还是逐字段继承。现有 `WithStreamOptions` 是整体替换，`WithStreamMode` 是逐字段修改，两者语义不同，新设计必须选定一种。随 Reliable/Realtime 删除的 `WithStreamMode`，以及 `WithStreamIdempotencyTTL` 是否保留，也需要写明。
- **缺失的配置**：GracefulStop 的收尾预算（§7.2 提到“预算”但没有配置项）；方法级的调用截止时间（§3.1 说“声明的调用截止时间还可更早”，但没有给出声明入口）；单调用配额（见 2.3）；输入空闲期限（见 2.7）。
- **校验规则**：“负值及相互冲突的预算在启动时拒绝”过于笼统，应列出具体的不等式，例如 `BufferBytes ≥ MaxEncodedFrameBytes`、`BatchBytes + N × 开销 ≤ Extra 批次上限`、`Recv.BufferMessages ≥ Recv.BatchMessages`。

### 10.2 内部与部署参数（§9.3）

- **所有权不清。** 这些参数“不放入业务 StreamConfig”，但由 SDK 以 create-only 方式创建资源。于是第一个启动的 SDK 实例决定了 Stream 的配置，配置不同的其他实例只会在读回校验时失败。建议资源由部署初始化任务创建（类似现有的 `extra-init`），SDK 只做读回校验，参数的唯一来源是部署配置。
- `Replicas = 3`、`StreamMaxBytes = 1 GiB`：见 2.2。
- `AckWait = 60s`：见 3.5。
- `HistoryConsumerIdleTTL = 60s`：见 2.5。
- `MaxDeliver = -1`：ACK 前移后，重投只来自网络或进程故障，无限重投是合理的。但损坏记录会导致 DataLoss，由实例自己退出，不会被 TERM，这条记录会在每次恢复时重复触发 DataLoss。应写明这是有意为之的“毒消息阻塞整个方向”，并给出诊断方法（第 6.4 节的工具）。

## 11 示例审查

### 原文 §10.1 配置示例

- 示例把几乎所有字段都填成了默认值，而注释说“仅覆盖需要调整的字段”，两者矛盾。示例应该只展示 `Address`、`TLSConfig` 以及一两个常见的覆盖项。
- `CompletionCheckInterval`、`WaitTimeout` 出现在业务示例中，与 §9.1 的原则冲突（见本文 10.1）。
- 没有展示方法级的 `WithStreamOptions`。

### 原文 §10.2 Client stream 与 Server stream

- 正文要求“提前返回也要释放本次调用资源”，但 `upload` 在 Send 失败时直接返回，既没有取消，也没有 CloseSend，后台的生产者、接收 goroutine 和 Fetch 会一直存在，直到调用方的 ctx 结束。示例应包含 `ctx, cancel := context.WithCancel(ctx); defer cancel()`，同时正文需要说明“终态之后 cancel 不会触发 CancelRun”（见 4.4）。
- `watch` 写法正确。

### 原文 §10.3 Bidi

- 写法正确：用 `callCtx` 保证 `ChatHellos` 失败时 errgroup 的 ctx 也会被取消。建议加一行注释说明为什么需要两层 ctx，否则读者很可能把 `callCtx` 当作多余的代码删掉。
- 应提醒读者：这里的 CloseSend 要等待持久化的 EOF，可能阻塞到重试结束，比原生 gRPC 慢得多，不适合放在对延迟敏感的路径上同步调用。

### 原文 §10.4 历史与迭代器

- `restore` 和 `historyEdges` 都以 `_ = reply.Count`、`_ = first.Sequence` 结尾，没有展示真实用法，读者无法从中学到“如何根据历史恢复进度”。
- `historyEdges` 把 `Sequence` 当作有业务意义的值来展示，与 §4“Sequence 不是业务计数”的说明相悖，容易诱导误用。
- `LastMessage` 当前是 O(n) 并且会新建 consumer，这个示例的成本不明显，应该在示例旁注明。

### 原文 §10.5 Bloom 预筛

- 复杂度是 O(n²)，见 2.6。
- 用 `Reply.message` 作为“稳定、唯一的业务键”。在 `demo.proto` 中这个字段是“业务文本”，并不唯一，示例的前提与 proto 不一致。应在 demo.proto 的 Reply 中增加 `id` 字段，或改用 `child_run_id` 这类真正唯一的字段。
- `bloom.MayContain(key)` 需要处理 error，原因是参数类型为 `any`，类型化后可以去掉（见 2.6）。

### 缺失的示例

- **Worker 端 handler**：三种形态都没有。尤其缺 client streaming 的 `SendAndClose`，以及 bidi handler 在重试时如何使用请求历史。
- **恢复**：`client.StreamRef` 的获取、序列化与持久化、`conn.ResumeStream` 的调用、`WithResumeSend` 的使用。恢复是本设计的核心卖点之一，却没有任何示例。
- **错误处理**：如何区分 io.EOF、业务 status、运行失败、过期、竞争失败，以及 Header/Trailer 的读取。

## 12 文档结构与链接

- 两个跳转文件中的链接文字是“NATS Client Stream Server Stream 与 Bidi Stream 设计”，与目标文档的实际标题“RPC 流与历史读取设计”不一致。
- 相对链接 `../../../../internal/services/extra/...`、`../../internal/rpc/...`、`../../task/checkpoint.go`、`../extra-streams-implementation-plan.md` 都能解析到实际文件。
- P0 引用的依赖版本（nats-server 2.15.1、nats.go 1.54.0、grpc-go 1.84.0）与根目录和 `sdks/wego` 的 go.mod 一致；示例用到的 `golang.org/x/sync` 在 `sdks/wego/go.mod` 中目前只是 indirect，示例落地时需要改为直接依赖。
- 全文约 700 行，否定式的限定句（“不能”“不保证”“不得”）超过 100 处，规则散落在各节。建议按“合同 → 资源 → 生产 → 消费 → RPC 映射 → 历史 → 配置 → 可观测 → 示例 → 验收”重排，每条规则只出现一次。

## 13 修改清单（按优先级）

1. 用 Msg-Id 加 `GetMessage(sequence, subject)` 重写 §5.3 的结果未知核对，每次网络尝试使用新 batch_id（2.1）。
2. 在 §3.1 增加容量模型；方向完成后回收 consumer，consumer 用 R1；与 Extra 的 MaxConcurrentRPCs、MaxPublishBatchesInFlight、MaxStoreBytes 以及单节点部署对齐（2.2）。
3. 增加单调用配额 `MaxMsgsPerSubject + DiscardNewPerSubject`，并写明背压语义（2.3）。
4. 明确 Fetch 的增量 ACK 与入 chan，加入低水位，调整接收缓冲默认值（2.4）。
5. 历史读取改用 Extra 的只读扫描 RPC，或 GetMessage 方案，删除临时具名 consumer（2.5）。
6. 删除 Bloom 和 Contains，改为类型化的精确 `Keys`；修正示例 10.5（2.6）。
7. 增加输入空闲期限或客户端活性信号（2.7）。
8. 新增可观测性章节：指标、日志字段、trace、诊断工具、告警（第 6 节）。
9. 增加错误分类表和终态判定表；要求 Extra 返回可区分的 ResourceExhausted reason（第 4 节）。
10. DATA 改用二进制 protobuf，统一大小预算（3.1、3.2）。
11. 删除迁移相关内容和资源名中的版本号；删除 Initializing/Ready、独立 GC、DescriptorTTL；基数为 1 的方向改为直接读取（第 7 节、3.6、3.9）。
12. 统一命名：`RequestHistory`/`ResponseHistory`、点分 subject、`All`、`Linger`/`Timeout`、`Retention`（第 9 节）。
13. 精简公开配置并补齐缺失项；明确部署参数的所有权（第 10 节）。
14. 补齐 Worker handler、恢复、错误处理三类示例，修正现有示例（第 11 节）。
15. 为性能验收设定量化目标（第 5 节）。
