# hatchet-extra 实现计划

日期：2026-10-15。依据：[RPC 流设计评审](nats-rpc-stream-design-review.md)。范围只覆盖 Extra 必须先提供的能力。SDK 的方向协议、历史 API 和迁移不在本计划内。

`cmd/hatchet-extra/main.go` 不实现协议。它解析 flag，填 `extra.Config`，再调用 `extra.New`。只改默认值解决不了错误无法区分、批次逐条等回复、历史扫描必须建 durable consumer 这三个问题。入口、服务和部署要同一批改完。

## 0 评估与实施状态

2026-10-15：P0–P3 已实现，服务从 26 个 RPC 扩展为 28 个；SDK 协议仍不在本次范围。验收命令与本地镜像见 [Extra 验收记录](../../../../internal/services/extra/TEST_REPORT.md)。

| 阶段 | 交付 |
| --- | --- |
| P0 | reason details、鉴权 GetLimits、三类独立入场预算、CLI/环境配置与数据库初始化前的校验 |
| P1 | 原子批次窗口流控、首条能力确认、可配置提交/放弃预算、native_batch_id |
| P2 | 精确 subject 的有限 Read、双预算、临时 AckNone consumer、独立取消清理及 durable 状态隔离 |
| P3 | 单节点副本数/存储预算限制、Compose 参数、公开桩重生成、回归与本地部署 |

评估后明确以下实现细节，后续章节中的目标按此解释：

1. 原子批次首条必须等待启动确认，再发送后续窗口；这符合 [ADR-50](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md)。第 1、17、33、49、65、81、97 条及第 100 条 commit 共等待 8 次回复。窗口为 1 时等待 100 次。
2. NATS 2.15.1 的 `JetStreamAccountLimits.MaxConsumers` 在单节点消费者创建逻辑中按 `mset.numLimitableConsumers()` 检查，即每个 Stream 的限制，不能映射到账户总数。按 §3.5 的兜底约定，不配置这个伪账户总上限，GetLimits.max_consumers 返回 0，正值请求在启动日志说明未强制。原生每 Stream 默认 1000 及显式 Stream.MaxConsumers 仍有效。SDK 自行管理 scope 总预算。
3. `Read` 的 max_messages/max_bytes 限制整个 RPC，超出时正常截断，不是持续扫描的内部页大小。显式 end 同样受入场时的尾部快照约束，防止请求未来序号后跟随新消息。SDK 分页时固定原 H0，按最后实际位置+1 继续。临时 consumer 创建结果未知也尝试删除，取消不取消独立清理预算，30 秒空闲回收兜底。
4. 字节预算固定使用 `sum(m.Size()+PublishBatchPerMessageOverhead)`，常量为 256。m.Size 含物理 subject 与 typed headers，不仅是载荷；SDK 可按 namespace 长度计算物理前缀的字节数，无须知道前缀内容，见 [Extra API 说明](../../../../internal/services/extra/README.md)。32×4 MiB 是原子批次计费缓冲预算，不代表进程总 RSS。
5. 副本数和相应存储介质预算校验同时覆盖 KV，避免借 KV 的底层 Stream 绕过单节点约束。Go Config 的 MaxConsumers=0 采用默认请求值，-1 表示不请求；CLI 仍按计划用 0 表示不请求。

专项测试覆盖全部 reason、原子性、检查点数量、deadline、原生放弃超时、native ID 读回、Read 范围/预算/取消/快照/租户/清理，以及旧、新限额参数互斥。测试与生成产物已纳入当前工作树。

## 1 目标

| 评审问题 | Extra 交付 |
| --- | --- |
| ResourceExhausted 同时表示名额耗尽、配置超限和存储满 | `NatsError.reason` 固定枚举，SDK 按 reason 决定重试或失败 |
| 没有限额查询，SDK 无法在启动时对齐预算 | 鉴权后的 `GetLimits` |
| 批次非 commit 消息逐条 `RequestMsg`，100 条就是 100 次往返 | 中间消息不等回复，按间隔做流控；只有 commit 等最终 PubAck |
| `commitBatch` 固定再套 30 秒，长于 NATS 默认批次放弃时间 10 秒 | 提交预算可配置，且不超过客户端 deadline |
| 批次字节按 `m.Size()+256` 计，SDK 按裸字节计会刚好超限 | `GetLimits` 公布每条开销，服务端继续用同一公式 |
| 历史读取只能建具名 durable consumer | `Read`：精确 subject、序号闭区间、服务端临时 consumer，RPC 结束即删除 |
| 128 路长期流、8 个在途批次、1 GiB 账户存储 | 拆开 unary / stream / read 名额；提高批次名额默认值；创建 Stream 时拒绝超过账户存储和副本数大于 1 |
| 单节点却允许 `Replicas=3` | 创建 Stream / Consumer 时 `Replicas>1` 直接拒绝 |

不改现有 26 个 RPC 的字段编号。新增字段只追加。不实现 CallID、EOF、Bloom，也不负责回收 SDK 的方向 consumer。

## 2 main.go 要改的参数

现有 flag 与 `extra.Config` 一一对应，见 `main.go` 第 62–83 行和 `server.go` 的 `Config`。本次把一个限额拆成三类，并补上提交预算。

| flag | 环境变量 | 现默认 | 新默认 | 含义 |
| --- | --- | --- | --- | --- |
| `--max-concurrent-unary` | `EXTRA_MAX_CONCURRENT_UNARY` | 与 stream 共用 128 | 128 | Get、Ack、GetLimits、资源查询 |
| `--max-concurrent-streams` | `EXTRA_MAX_CONCURRENT_STREAMS` | 128 | 1024 | Fetch、PublishBatch、PublishAsync、Watch |
| `--max-concurrent-reads` | `EXTRA_MAX_CONCURRENT_READS` | 无 | 64 | 只给 `Read`，不占用 Fetch 名额 |
| `--max-publish-batches-inflight` | 沿用现有 flag | 8 | 32 | 同时缓冲的原子批次。32 × 4 MiB = 128 MiB 上限 |
| `--batch-commit-timeout` | `EXTRA_BATCH_COMMIT_TIMEOUT` | 代码写死 30s | 8s | 单次 `commitBatch` 上限，必须小于 NATS 批次放弃时间 |
| `--batch-abandon-timeout` | `EXTRA_BATCH_ABANDON_TIMEOUT` | NATS 默认 10s | 10s | 写入 embedded server 的 `JetStreamLimits.MaxBatchTimeout` |
| `--publish-batch-flow-control` | `EXTRA_PUBLISH_BATCH_FLOW_CONTROL` | 每条都等回复 | 16 | 每 N 条中间消息等一次空回复；1 表示保持现状 |
| `--max-consumers` | `EXTRA_MAX_CONSUMERS` | 未限制 | 10000 | 默认账户的 JetStream consumer 上限；0 表示不在服务端强制 |

保留 `--max-concurrent-rpcs` 一个版本：若同时出现，启动失败，避免两套数字。`validateConfig` 增加：

- `batch-commit-timeout > 0` 且 `< batch-abandon-timeout`。
- `max-publish-batch-bytes >= max-message-bytes + 256`。
- `max-concurrent-streams >= 1`，上限仍为 4096；`max-publish-batches-inflight` 上限从 128 提高到 1024，避免默认 32 贴着旧上限。
- 数值 flag 在连接数据库之前校验。现在 `run` 先 `InitDataLayer` 再 `extra.New`，非法限额会先占用数据库连接。

启动成功打一条 info，只含地址和上述限额，不含密钥、token、数据库连接串。

`sdks/wego/deployment/compose/docker-compose.yml` 里 `hatchet-extra.command` 同步这些 flag。未写的项用新默认值，不要在 compose 里继续写死 `--max-publish-batches-inflight=8`。

## 3 服务改动

### 3.1 错误 reason

`NatsError` 追加字段，不改已有编号：

```protobuf
message NatsError {
  int32 grpc_code = 1;
  string message = 2;
  optional int32 api_code = 3;
  optional uint32 nats_error_code = 4;
  string reason = 5;
}
```

reason 用稳定英文常量，不把句子当协议。

| reason | code | 调用方动作 |
| --- | --- | --- |
| `BATCH_CAPACITY` | ResourceExhausted | 退避重试。来自 `batchSlots` 满 |
| `BATCH_MESSAGE_LIMIT` | ResourceExhausted | 永久失败。单批条数超过服务配置 |
| `BATCH_BYTE_LIMIT` | ResourceExhausted | 永久失败。含每条 256 字节开销 |
| `ACK_BATCH_LIMIT` | ResourceExhausted | 永久失败 |
| `MESSAGE_TOO_LARGE` | ResourceExhausted | 永久失败。`statusTooLarge` 与 `nats.ErrMaxPayload` |
| `RPC_CAPACITY` | ResourceExhausted | 退避重试。unary 名额 |
| `STREAM_CAPACITY` | ResourceExhausted | 退避重试。长期流名额 |
| `READ_CAPACITY` | ResourceExhausted | 退避重试。Read 名额 |
| `FETCH_BUFFER` | ResourceExhausted | 退避重试。Fetch 订阅缓冲满 |
| `STORE_EXHAUSTED` | ResourceExhausted | 退避后仍失败则终止。NATS 存储、内存、账户资源、Stream `MaxBytes` |
| `CONSUMER_LIMIT` | ResourceExhausted | 终止并告警。达到 `--max-consumers` |
| `REPLICAS_UNSUPPORTED` | FailedPrecondition | 永久失败。请求副本数大于 1 |

`helpers.go` 的 `rpcError` 在映射 429 和 `JSStorageResourcesExceeded`、`JSStreamMaxStreamBytesExceeded`、`JSMaximumConsumersLimitErr` 时填上表中的 reason。`publishError` 保持 `FailedPrecondition`，reason 用 `PUBLISH_PRECONDITION`。本地 `status.Error` 改为带 details 的构造函数，例如 `exhausted(reason, message)`，避免有的路径有 details、有的路径只有字符串。

`Aborted`（同一 native batch id 已在途）保持现有 code，reason 为 `BATCH_IN_FLIGHT`。SDK 若按计划每次尝试更换 `batch_id`，这条不应出现。

### 3.2 GetLimits

挂在现有 `StreamService` 上，避免再增加一个 service：

```protobuf
rpc GetLimits(google.protobuf.Empty) returns (ServiceLimits);

message ServiceLimits {
  uint64 max_message_bytes = 1;
  uint32 max_fetch_messages = 2;
  uint64 max_fetch_bytes = 3;
  uint32 max_publish_batch_messages = 4;
  uint64 max_publish_batch_bytes = 5;
  uint32 publish_batch_per_message_overhead = 6;
  uint32 max_publish_batches_inflight = 7;
  uint32 max_ack_batch_messages = 8;
  uint32 max_concurrent_unary = 9;
  uint32 max_concurrent_streams = 10;
  uint32 max_concurrent_reads = 11;
  int64 max_store_bytes = 12;
  int64 max_memory_bytes = 13;
  int64 batch_commit_timeout_ns = 14;
  int64 batch_abandon_timeout_ns = 15;
  uint32 publish_batch_flow_control = 16;
  uint32 max_consumers = 17;
  bool single_node = 18;
}
```

`max_message_bytes` 的含义保持现状：编码后的 `len(m.Data)` 与 KV value，不是解码后的业务对象。`publish_batch_per_message_overhead` 固定返回 `batch.go` 里的 256，并把该数字提成 `extra` 包的导出常量 `PublishBatchPerMessageOverhead`，禁止两处各写一个魔数。

`single_node` 恒为 true。鉴权与普通 unary 相同，未带 token 返回 Unauthenticated。响应不含路径、密钥和账户名。

### 3.3 PublishBatch

`commitBatch` 目前对每一条调用 `RequestMsgWithContext`。改为：

1. 非 commit 消息去掉 reply，用 `PublishMsg` 发出。
2. 每 `publish-batch-flow-control` 条仍用 `RequestMsg` 等空回复，用来发现连接失败，不等 PubAck。
3. 只有最后一条设置 `Nats-Batch-Commit: 1` 并等待最终 PubAck。校验 `BatchID`、`Count`、`Sequence` 的现有逻辑不变。
4. 预算改为 `context.WithTimeout(ctx, cfg.BatchCommitTimeout)`。父 context 更短时以父 context 为准。
5. `PublishBatchAck` 追加 `string native_batch_id = 4`，返回现在写入 `Nats-Batch-Id` 的 hex。SDK 不需要反推 `sha256(物理前缀 + batch_id)`。物理前缀仍然不返回。

`server.go` 的 `New` 在 `ns.Options` 上设置 `JetStreamLimits.MaxBatchTimeout`。设置前确认 2.15.1 的 embedded 选项名；若字段不存在，启动失败，不用默认 10 秒假装已配置。

名额耗尽仍在读第一条请求之前返回，不缓冲请求体。reason 用 `BATCH_CAPACITY`。

### 3.4 Read

```protobuf
rpc Read(ReadRequest) returns (stream NatsRawStreamMsg);

message ReadRequest {
  StreamRef stream = 1;
  string subject = 2;
  uint64 start_sequence = 3;
  uint64 end_sequence = 4;
  uint32 max_messages = 5;
  uint64 max_bytes = 6;
}
```

行为：

- `subject` 必须是精确 subject，拒绝 `*` 和 `>`。租户前缀仍由 `address.subject` 加上，调用方只传逻辑名。
- `start_sequence == 0` 从 1 开始。`end_sequence == 0` 表示读到当前尾部为止，不跟随之后的新消息。两者都非 0 时要求 `start <= end`。
- `max_messages`、`max_bytes` 缺省时用 Fetch 的默认上限，且不能超过服务端上限。
- 服务端创建 consumer：精确 filter、`DeliverAll` 或 `StartSequence`、`AckNone`、`InactiveThreshold` 30 秒。名字由服务端生成，不接受客户端命名。
- 按到达顺序写出 header 和 data。序号大于 `end_sequence` 的消息不发送，然后正常结束流。
- `defer` 删除该 consumer。取消、发送失败、客户端断流都走同一条删除。删除失败打 error 日志，带 stream 和 consumer 名，不把删除失败伪装成扫描成功。
- 不修改同 subject 上已有 durable consumer 的 `AckFloor`。
- 入场走独立的 read 名额，不进 `streamAdmission` 的 1024。实现上给 `Read` 单独拦截，或在 handler 入口换用 read slots，避免 Fetch 被扫描占满。

这是历史快照的传输原语。SDK 仍负责 H0、类型校验和业务解码。Extra 不解释帧内容。

### 3.5 创建资源时的部署约束

`CreateOrUpdateStream` 与 `CreateOrUpdateConsumer`：

- `Replicas > 1` 返回 `REPLICAS_UNSUPPORTED`。当前进程是 `DontListen` 的单节点，见 `server.go` 的 `New`。
- Stream `MaxBytes` 小于 0 且不是“不限制”的既有语义时拒绝；大于 `MaxStoreBytes` 时返回 `STORE_EXHAUSTED`。一个租户的 RPC Stream 不能声明超过整个账户的磁盘预算。
- 能映射到默认账户的 `MaxConsumers` 就用 `--max-consumers` 设置。映射不了就在 `GetLimits.max_consumers` 返回 0，并在启动日志写明“服务端未强制 consumer 上限”。不要在 SDK 文档里假装 Extra 已经强制 10000。

### 3.6 Config 结构

`extra.Config` 增加 `MaxConcurrentUnary`、`MaxConcurrentStreams`、`MaxConcurrentReads`、`BatchCommitTimeout`、`BatchAbandonTimeout`、`PublishBatchFlowControl`、`MaxConsumers`。`MaxConcurrentRPCs` 保留给 `defaults`：两个新字段都为 0 时，两者都取它，避免测试里每个 `Config{...}` 都要改。`main.go` 始终显式填写新字段。

`auth.go` 的两个 interceptor 改为接收各自的 channel。`New` 里不再把同一个 `MaxConcurrentRPCs` 同时传给 unary 和 stream。

## 4 实施顺序

### P0 限额与错误

1. 改 `extra.proto`，重新生成 `pkg/extra/v1`。生成命令沿用仓库现有任务，不手改 `.pb.go`。
2. 拆 Config 和 interceptor，改 `main.go`、`defaults`、`validateConfig`。
3. 实现 `exhausted` / `GetLimits`，给现有 ResourceExhausted 路径补 reason。
4. 测试：每个 reason 一条；`GetLimits` 与传入 Config 一致；无 token 被拒绝；非法 flag 在 `InitDataLayer` 之前返回。

### P1 批次提交

1. 改 `commitBatch` 的发送循环和超时。
2. 设置 `MaxBatchTimeout`。
3. Ack 返回 `native_batch_id`。
4. 测试：100 条批次仍然全有或全无；中间消息失败不留持久前缀；commit 超时返回 DeadlineExceeded；`native_batch_id` 与读回的 `Nats-Batch-Id` 相同；flow control 为 1 时行为与现在一致。现有原子性用例保持通过。

### P2 Read

1. 实现 `Read` 和独立名额。
2. 测试：闭区间、空 subject、通配符拒绝、取消后 consumer 消失、并行 durable consumer 的 AckFloor 不变、跨租户 subject 不可见、超过 max_messages / max_bytes 时截断并正常结束。

### P3 部署约束与默认值

1. Replicas 和 MaxBytes 校验。
2. 尝试设置账户 consumer 上限。
3. 更新 compose command 和 `TEST_REPORT.md` 的限额说明。
4. 回归：`GOWORK=off go test ./internal/services/extra/... ./cmd/hatchet-extra/... ./pkg/extra/v1 -count=1`，以及同一范围的 `-race`。集成测试沿用现有 `TestLiveProcess`，断言新 RPC 可调用，健康检查仍然不带 token。

## 5 验收

- 批次名额满、单批超限、Stream 磁盘满三者的 gRPC code 可以同为 ResourceExhausted，details 里的 reason 不同。
- 客户端把 deadline 设为 2 秒时，`commitBatch` 不超过该 deadline，不再固定等到 30 秒。
- 100 条批次的发布往返次数约为 `ceil(99/flowControl)+1`，而不是 100。
- `Read` 结束后 `ListConsumers` 看不到这次扫描的 consumer。
- `Replicas=3` 的 `CreateOrUpdateStream` 失败，单节点服务仍能启动。
- compose 未覆盖的 flag 使用第 2 节的新默认值；`--max-concurrent-rpcs` 与新 flag 同时出现时进程退出。

## 6 不在本计划内

- SDK 的 Msg-Id 核对、单调用 `MaxMsgsPerSubject`、结束检查和指标。
- 把 Extra 做成多副本集群。
- 删除或改写已有 RPC 的字段编号。
- 对象存储 codec 的生命周期。那是 SDK 部署配置，不是 `hatchet-extra`。
