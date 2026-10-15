# RPC 流配置与运维参考

先读 [主方案](nats-rpc-stream-design.md)。本文按需查阅：配置全集、预算校验、部署参数和可观测性。原章节编号保留，便于定位已有评审引用。

<a id="s9"></a>

## 9 配置与命名

<a id="s9-1"></a>

### 9.1 公开命名

公开 API 按流的用途命名，组件选择留在内部适配层。实例配置沿用 runtime.WithStream(runtime.StreamConfig)，方法覆盖沿用 runtime.WithStreamOptions，不增加第二套入口。

| 名称 | 职责 |
| --- | --- |
| runtime.StreamConfig | 流服务连接和实例默认配置 |
| runtime.StreamSendConfig | Send 缓冲、组批及写入等待 |
| runtime.StreamRecvConfig | Recv 缓冲、批量接收及可选空闲期限 |
| runtime.StreamHistoryConfig | 历史扫描、收集与 Bloom 预算 |
| runtime.StreamOptions | 方法级 Send、Recv、History 及消息大小覆盖，不覆盖服务连接 |
| model.StreamRef | 调用恢复引用，文中统一使用此名 |
| model.StreamHistory / HistorySnapshot | 历史操作对象与固定快照 |
| model.StreamMessage / StreamBloom | 历史消息与过滤器 |

结构字段统一使用 Send、Recv、History，与业务的 Send/Recv 操作对应；不使用 Producer/Consumer 表示公开配置分组。Go 类型和字段使用 PascalCase，RPC 编码字段遵循 protobuf 的 snake_case，资源名与 Go 类型名分别处理。

<a id="s9-2"></a>

### 9.2 SDK 配置

下列值是目标默认值。实例配置的零值采用默认值。方法 StreamOptions 的字段为 MaxMessageBytes *int、Send *StreamSendConfig、Recv *StreamRecvConfig、History *StreamHistoryConfig：nil 继承实例对应项，非 nil 的配置组完整覆盖该组，组内零值按 SDK 默认值补齐。例如 Send 指向空配置会将 BatchWait 重置为默认 0，而非继承实例的正值。多次 WithStreamOptions 注册同一方法时后一次完整替换覆盖声明。负值和冲突预算启动时拒绝。WithStreamMode 随两套传输分支移除；WithStreamIdempotencyTTL 保留提交幂等职责。

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| Address / TLSConfig | 使用流时地址必填 | 流服务端点；复用实例认证信息 |
| MaxMessageBytes | 1 MiB | 业务 protobuf 未压缩序列化大小上限；不是 Go 堆对象大小 |
| Send.BufferMessages / BufferBytes | 1000 / 8 MiB | 同时限制未提交的本地消息 |
| Send.BatchMessages / BatchBytes / BatchWait | 100 / 4 MiB / 0 | 按条数和完整计费字节拆批；正的 BatchWait 才等待聚合 |
| Send.WriteTimeout | 10s | 一次后台写入尝试的等待，整体重试仍受 Context 限制 |
| Send.MinRetryWait / MaxRetryWait | 100ms / 5s | 退避范围，带抖动 |
| Recv.BufferMessages / BufferBytes | 200 / 8 MiB | 含在途响应和待入 chan 的预留 |
| Recv.BatchMessages / BatchBytes | 100 / 4 MiB | 一次后台接收请求的条数与编码字节上限 |
| Recv.IdleTimeout | 0 | 可选的对端输入空闲期限，按 [§6.2](nats-rpc-stream-protocol-reference.md#s6-2) 计时 |
| History.ScanTimeout / MaxScanMessages / MaxScanBytes | 60s / 100万 / 1 GiB | 一次操作累计扫描预算，包含 Bloom 的两遍读取 |
| History.MaxConcurrentScans | 每实例 4 | 超出时等待 Context 或返回资源限制错误 |
| History.AllMessageMaxMessages / AllMessageMaxBytes | 1万 / 32 MiB | AllMessage 内存收集上限，大历史使用 Iterator |
| History.BloomFalsePositiveRate / BloomMaxBytes | 0.0001 / 16 MiB | 误判率及位图内存上限 |
| History.IndexMaxBytes | 32 MiB | 一个句柄内所有 Contains 精确索引的总预算 |

缓冲至少能容纳一条解码后最大消息及编码开销。实际接收条数按剩余内存预算缩小；存在压缩时，不能只用网络字节数证明内存有界。原始字节与解码对象的临时重叠也计入预算。

业务消息大小、完整编码帧大小和进程内存预算分别校验。ProtoJSON 可能膨胀；即使 proto.Size 不超过 MaxMessageBytes，编码帧仍可能超限，必须在 Send 接纳前返回 ResourceExhausted。不会通过无限扩容或自动切换对象存储绕过限制。codec 的解码长度、对象分配上界和所有重叠缓冲要一并进入 P0/P2 测试。

启动校验包括：Recv.BufferMessages ≥ Recv.BatchMessages；缓冲字节能容纳一条最大编码/解码记录的重叠；各批条数不超过 GetLimits；发送批次按 Σ(m.Size() + publish_batch_per_message_overhead) 计费，控制头与物理 subject 长度采用适配层提供的精确映射或已验证上界，不把业务 payload 长度当批次字节。单条可发送的必要条件是计费大小不超过单批预算。方法预算只能收紧部署硬上限。BloomMaxBytes 即使在默认扫描条数下不触发，也保护用户扩大扫描规模或降低误判率后的内存。

当前 Extra 地址映射中租户编码固定 32 字符，namespace 最长 64 字节经 base32 编码，物理 subject 前缀最多 139 字节；SDK 可以按这一经过版本测试的长度上界预算，无须知道真实物理前缀。新增的批次头与回复地址另按 GetLimits 返回的开销计入，P0 验证两端公式一致。

<a id="s9-3"></a>

### 9.3 内部映射与部署配置

以下是 NATS 适配和部署参数，不放入业务的 StreamConfig，不要求业务示例填写资源名、复制数或原生 consumer 参数。方法级配置也不修改共享存储策略。

| 参数 | 目标默认值 | 映射或约束 |
| --- | --- | --- |
| RPC 专用 namespace / Stream / 状态 bucket | rpc / wego_rpc / wego_rpc_state | 部署初始化；SDK 只读校验，版本留在调用描述 |
| Retention / DescriptorTTL / RetentionSafetyMargin | 24h / 48h / 5m | 保留与恢复期限 |
| Replicas | 当前单节点为 1 | 文件持久化，不能宣称单节点故障容错 |
| StreamMaxBytes / ScopeMaxConsumers / MaxMsgsPerSubject | 部署必填 | 按 [§3.1.1](nats-rpc-stream-protocol-reference.md#s3-1-1) 计算，不能把 1 GiB 和 10000 当通用生产容量 |
| MaxEncodedFrameBytes | 1 MiB | 完整编码结果含控制信息；超限返回错误 |
| AckWait / MaxAckPending / MaxDeliver | 60s / 1000 / -1 | AckExplicit，不使用业务等待续期 |
| AckBatchMessages / AckBatchWait | 100 / 1ms | ACK 调度参数，不是业务处理确认 |
| FetchWaitTimeout | 5s | 后台 Pull 长轮询上限，不是消息交付等待 |
| CompletionCheckMinInterval / MaxInterval / Timeout | 1s / 30s / 3s | 未知 EOF 的独立检查；已知 EOF 使用 [§6.2](nats-rpc-stream-protocol-reference.md#s6-2) 的短退避 |
| HistoryReadMessages / HistoryReadBytes | 100 / 4 MiB | Read 每页预算；同时受服务限额约束 |
| HistoryConsumerIdleTTL | 由 Extra 管理 | 扫描 RPC 退出清理，空闲回收兜底 |
| GracefulStopTimeout | 30s | 实例收尾预算，可由部署调整 |

Send.BatchMessages/BatchBytes 映射原子批量发布；Recv.BatchMessages/BatchBytes 映射普通 Pull 的双预算。服务端返回的序号映射为 StreamMessage.Sequence，应用不依赖原生 consumer 名或 header。AckWait 保留已确定的 60s 默认值，它覆盖网络故障、ACK 排队和进程停顿而非业务处理；未 ACK 崩溃消息可能等待至该期限后重投。是否下调由故障恢复目标和延迟分布决定。

Extra 的 Fetch 默认上限为 100 条、4 MiB；max_bytes_limit 可与 max_messages 同时限制。启动通过 GetLimits 和资源配置查询校验实际部署预算；Read 等必需接口另做能力探针。服务端不支持这些能力时返回错误，不运行中更换收发语义。当前工作树已有 GetLimits、错误 reason 和批次流水化改动，是否已发布/部署以 Extra 验收记录为准，不将源码存在等同于服务可用。

<a id="s9-4"></a>

### 9.4 可观测性

指标使用 wego_rpc_stream_ 前缀，只使用已注册 method、direction、role 和固定 reason/outcome/code 标签；CallID、RunID、subject、业务键放日志，禁止作为指标标签。

| 指标组 | 必须覆盖 |
| --- | --- |
| calls_active、send_queue_messages/bytes、recv_buffer_messages/bytes | 并发与本地预算占用 |
| publish_batch_size、publish_duration_seconds、publish_retries_total | 实际组批效果、时延与分类重试 |
| publish_unknown_total、publish_conflicts_total、ack_failures_total | 核对结果、竞争与 ACK 故障 |
| fetch_messages、redeliveries_total、completion_checks_total | 小批次、重投与结束查询负载 |
| terminal_total、completion_latency_seconds | 终态来源与结束时延；远端 EOF 时间只在时钟可比时使用 |
| history_scans_total、history_scan_messages、history_index_bytes | 扫描成本、索引复用和内存 |
| consumers_active、consumers_deleted_total | 方向/扫描 consumer 存量、清理结果 |

结构化日志在建立、恢复、终态记录 info，错误与未知结果核对记录原因；字段包括 call_id、run_id、attempt、direction、expected、逻辑批次 ID、stream_seq，不逐条记录载荷或密钥。Trace context 通过调用描述/任务元数据连接 Client 与 Worker，批次、核对、历史扫描用采样子 span，空闲轮询不创建无界 span。

诊断入口按 StreamRef 汇总调用描述、双向尾部、方向 consumer 的 AckFloor/NumPending/NumAckPending/NumWaiting、运行状态和资源限额。NumWaiting 大于 1 是重复等待读取的线索，不能单凭它证明存在几个业务实例。存储/consumer 配额默认在 80% 使用率告警，清理失败、未知结果比例、重投和等待时长按部署 SLO 告警。删除旧 CLAIM/Checkpoint 专属指标时同时接入上述指标，不留下观测空档。
