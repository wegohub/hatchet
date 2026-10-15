# wego 接入 hatchet-extra RPC 流实施计划 历史归档

> 已替换：RPC 流重构统一以 [NATS Client Stream Server Stream 与 Bidi Stream 设计](design/nats-rpc-stream-design.md)为准。下文保留历史讨论，不作为当前实施或验收依据；其中 CLAIM/OWNER、reader generation、Recv 后提交位置、ATTEMPT_END 和强制 Checkpoint 等方案已被替换。通用 KV、WorkerEvent 与原生事件 API 的独立职责按新方案的范围说明处理。

> 复核日期：2026-10-15。状态：设计稿，尚未实施本重构。
> 当前 SDK 为 0.4.27、消息协议为 6；目标 SDK 为 0.5.0、消息协议为 7。
> 本次修改仅完善计划，不代表目标版本已经发布或下述验收已经通过。

## 一、评估结论与本次范围

采用 Hatchet 调度、hatchet-extra 保存消息与消费状态的方向可行。Extra 已提供必要的发布、条件发布、读取、ACK 和 KV CAS 接口，但它不会自动解决 RPC 执行所有权、业务处理确认和跨系统事务。

原计划需要修正的关键问题：

| 原计划的处理 | 风险 | 本计划的处理 |
|---|---|---|
| AckAll，并在交付一帧后确认所属批次 | 批次剩余消息可能尚未交付；重投时累计 ACK 还可能越过缺口 | AckExplicit；只确认完全提交的消息批次，批次内位置另行保存 |
| 发现空洞立即删除并重建同名 consumer | 迟到 Fetch/ACK 可能操作重建后的资源；服务端 ACK floor 不再代表原位置 | 有界修复；需要重建时使用新的 consumer 实例名 |
| 所有后来的 writer 都能发布 ATTEMPT 接管 | 同代次重复投递会抢占；旧执行也可能重新取得写权 | 区分业务 retry epoch 与物理 writer；同代次只允许一个获胜执行 |
| 遇到更高代次返回不可重试错误 | 旧执行的失败上报可能影响当前运行 | 保留执行门禁；落选/失去写权的执行不报告终态 |
| END 表示整个任务完成 | END 可能来自失败 attempt，或发布成功但 Hatchet 未接受结果 | ATTEMPT_END 与引擎最终清单共同确认终态 |
| Replay 直接丢弃重新产生的前 L 条输出 | 非确定性变化被隐藏，客户端收到拼接后的错误结果 | 默认业务显式恢复；可选 Replay 必须逐条核对历史 |
| InputMark 等于 Recv 次数，按它跳过输入 | 已 Recv 不等于处理完成；多输入一输出、并发处理会漏任务 | 不自动用 Recv 计数推进业务恢复位置 |
| 幂等冲突后自动续写输入 | 第二个调用者可以接管第一个调用的发送方向 | 默认只附着消费；输入续写使用独立、显式的恢复凭证 |
| 只有 RunID、OutputSeq 的恢复位置 | 缺少租户、绑定、物理位置、批次偏移和响应头状态 | 扩展现有 StreamResumeToken，严格验证身份及位置 |
| 删除 PutStream、task.StreamEvent、WorkerEvent | 破坏已确认的原生调用、降级及公开通知能力 | 按下文保留这些契约，迁移其传输 |
| 通过 go.work 导入未发布的 extra，然后跳过发行检查 | 本机通过，独立模块消费者无法构建 | 保留 GOWORK=off 门禁，SDK 使用可独立构建的生成客户端桩 |
| 明文 Wego 控制头、逗号拼接 CodecID | 与完整协议帧经过 codec 的约定冲突；合法 ID 可能含逗号 | 继续使用有序 repeated CodecID 和完整帧 codec |

本次交付包括三种 RPC 流、恢复、取消与关闭、必要的 Stream/KV 客户端，以及 durable Execute 消费流的接入。普通 unary、DAG、Batch、调度策略、命名规则和网络 gRPC 不另行重构。

## 二、核验基线与既有约定

### 2.1 代码现状

以下结论来自当前工作树，不能把计划接口当成已有能力：

- [stream_client.go](../internal/rpc/stream_client.go)：client/bidi 输入在 CloseSend 后一次提交；目前没有实时输入传输。
- [stream_worker.go](../internal/rpc/stream_worker.go)：一个流 RPC 对应一个普通 standalone task；client stream 最终响应保存于任务结果；server/bidi 可选 Reliable 或 Realtime；原生调用单独处理。
- [stream.go](../internal/rpc/stream.go) 和 [stream_checkpoint.go](../internal/rpc/stream_checkpoint.go)：目前禁止在 durable handler 中创建或恢复流；仅新增 extra 连接不会解除这个限制。
- [gated_execution.go](../internal/backend/gated_execution.go)：持久门禁负责 START 去重、执行身份和落选者不上报；不能随着旧传输一起删除。
- [completion.go](../internal/stream/completion.go)：已有“引擎结果＋匹配的结束帧＋完整输出”校验，必须保留这条不变量。
- [task/checkpoint.go](../task/checkpoint.go)：Worker 历史恢复；[client/stream.go](../client/stream.go)：客户端消费凭证；两者职责不同。
- [worker_event_subscribe.go](../internal/backend/worker_event_subscribe.go)：当前每个 Worker 都建立通知订阅，因而 unary Worker 也依赖 Hatchet Durable Streams；需要改为按能力启用。
- [FrameCodec](../internal/wire/log_codec.go)：完整 protobuf 帧经过有界 codec，发送链与按 ID 查找的解码注册表已分离，不需要重新发明 codec 包。

### 2.2 Extra 已有能力及限制

路径从本文所在的 `sdks/wego/docs` 计算：

- [服务实现](../../../internal/services/extra/README.md)
- [24 个 RPC 的 protobuf](../../../internal/services/extra/proto/v1/extra.proto)
- [已完成的 Compose 部署与验收](../../../internal/services/extra/TEST_REPORT.md)
- [Compose 配置](../deployment/compose/docker-compose.yml)

运行基线为官方 Hatchet v0.110.5、PostgreSQL MQ、NATS Server v2.15.1、nats.go v1.54.0。Extra 当前为单节点，已在 `localhost:7080` 提供 TLS gRPC。复用 Hatchet 租户 token，租户由鉴权结果确定，namespace 由请求 Scope 提供。

需要在 P0 固定为探针的语义：

1. PublishAsync 可以并发处理条目，不保证存储顺序等于 Send 顺序；有序方向先采用单一在途 Publish。
2. 当前 NATS 单节点实现先检查 expected-last-subject-sequence，再进行 message-id 去重。响应丢失后重试可能得到 WrongLastSequence，需要读回核对。此行为来自当前版本源码，不能假定所有服务版本和集群配置都一致。
3. ACK floor、delivered、StreamSeq、ConsumerSeq 各有不同含义；consumer 已投递到 20，不表示 20 已被业务处理。
4. Fetch 的 EOF 只表示本次拉取结束；AckWait 到期、断连和重投都不表示 RPC 结束。
5. GetMessage 支持物理序号和 last_subject；没有任意业务序号查询、倒序分页或 next_subject 接口。
6. CreateOrUpdate 会更新已有配置；不能把它无条件当成“只创建”。普通调用不能反复覆盖共享 stream 的保留期。
7. 默认单消息 1 MiB，Fetch 上限 64 条/4 MiB，普通等待不超过 30 秒，unary/streaming 入场预算分别为 128。Fetch 请求采用消息数或字节数的 oneof，不能声称一次请求同时设置二者。
8. 消息落盘确认、consumer ACK 和 KV CAS 是三个独立操作；Hatchet 任务提交与它们也没有共同事务。

官方参考：[Pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers)、[固定版本的条件发布实现](https://github.com/nats-io/nats-server/blob/v2.15.1/server/stream.go)。以本机探针验证固定版本的具体错误和重投行为。

### 2.3 保留的产品边界

- Server 继续使用 NewServer、注册 handler、Serve、Stop、GracefulStop；不新增控制 Worker、START/SESSION workflow。
- 网络 gRPC 直接执行 handler，不经过 Extra 或任务封包。
- 普通任务和 RPC 的协议 6 外形继续使用 `_wego/value/routing`；升级版本至 7，只增加流所需的可选字段。
- 原生 Console/官方 SDK 调用仍按显式响应契约处理；native 输出不执行业务 codec，server/bidi 使用 PutStream 发布业务 ProtoJSON，最终结果仅保存 sent_messages。
- task.StreamEvent 与原生实时订阅继续存在。
- Worker 事件通知是已公开能力，不能因仓库里使用次数少就删除。
- 不修改 Hatchet 调度、重试或 durable 引擎；必要的 Extra 协议补充单独列明、单独验证。

## 三、配置与模式选择

使用此前确认的命名：

```go
runtime.WithDurableExtra(runtime.DurableExtraConfig{
    Address:   "localhost:7080",
    TLSConfig: tlsConfig, // 本机部署须信任导出的证书；不设置 InsecureSkipVerify。
})

// 方法的任务策略可以明确禁用持久 Extra 流。
task.WithDisableDurableExtra()
```

- token 与 tenant 复用 Hatchet Runtime；Extra 使用独立连接，不能将 Hatchet gRPC 地址误作 Extra 地址。
- namespace 保持相同逻辑值；启用 Extra 时验证其名称限制，不改变未使用 Extra 的命名规则。
- TLS 配置在实例构造时复制。明确选择明文只能用于匹配的本地开发服务；已部署的 Compose 端口要求 TLS。
- Runtime 不使用全局连接。借用的 task.Conn 不关闭底层 Extra 资源。

模式在注册绑定和本次提交中确定：

| 条件 | 输入 | 输出与恢复 |
|---|---|---|
| wego + Extra + Reliable，任务未禁用 | client/bidi 使用 Extra 实时输入；server stream 内联一个请求 | Extra 持久输出，支持恢复 |
| 未配置 Extra，或任务明确禁用，或显式 Realtime | client/bidi 收齐数组后一次提交；server stream 内联请求 | PutStream；不提供持久恢复 |
| native 响应契约 | 一个请求或内联请求数组 | 原生 PutStream/单响应结果，不进入 wego 持久协议 |
| 网络 gRPC | 原生 gRPC | 原生 gRPC |

这是配置决定的降级。Extra 已启用后发生网络故障、鉴权失败或磁盘满，必须返回明确错误，不能运行中切换 PutStream 或本地 KV。

保留 Reliable/Realtime 和现有配置入口。缺少 Extra 时，注册绑定明确记录 realtime/inline 能力；客户端据绑定选择路径。恢复 API、手动确认等持久能力在该模式返回不支持。

纯 unary 或纯网络实例不建立 Extra 流资源。显式调用 conn.KV/Stream、注册通知回调或启用持久流后，才初始化相应能力。

## 四、资源、身份与统一帧

### 4.1 身份必须分开

| 身份 | 作用 |
|---|---|
| RunID | Hatchet 运行身份，供查询、恢复、取消 |
| TaskRunID | 当前 standalone 任务身份，必须从真实执行/查询获得，不能假定等于 RunID |
| WorkflowVersion/方法及 protobuf 类型 | 固定本次运行的注册契约 |
| RetryCount/epoch | Hatchet 的重试代次，不用本地重连次数代替 |
| input_generation | 输入发送者的接管代次，与 Worker 的 RetryCount 独立 |
| writer_nonce | 同一代次中一次物理执行/输入写者身份 |
| consumer_id | 一个独立逻辑消费位置，不等于运行身份 |
| reader_generation | 该消费位置的一次所有权；隔离旧 Fetch/ACK |
| StreamSeq | Extra stream 的物理序号，不同运行交错，允许跳号 |
| RecordSeq/DataSeq | 单方向协议记录序号/业务消息序号；控制记录不消耗 DataSeq |

SDK 自有的 stream/bucket 使用版本化保留名称，例如 `wego_rpc_v7`、`wego_state_v7`。输入输出 subject 使用 `rpc.<RunID>.in/out`；记录内部同时绑定 TaskRunID、方法及调用身份。consumer 使用稳定摘要加实例后缀，全部名称满足 64 字节限制，不能直接拼入任意长的业务 consumer 名。

同一租户、namespace 内共享底层 stream；不同 consumer 独立消费同一运行。SDK 保留名称检查仅防误用，同一租户凭证仍具有 Extra 管理权限，不把名称前缀当作安全边界。

### 4.2 安全初始化与保留期

- SDK 启动时读取并验证资源配置；已存在且不兼容时失败，不自动修改。
- 为避免首次创建竞争覆盖配置，P0 为 Extra 的 CreateOrUpdateStream/CreateOrUpdateBucket 增加可选 `create_only` 字段；委托原生 Create，存在时返回 AlreadyExists，随后读回核验。不增加 RPC 数量，也不更改默认 upsert 语义。
- 内部 stream 采用 FileStorage、LimitsPolicy、DiscardNew。不能采用消费后删除消息的 WorkQueue/Interest retention，否则独立回放和多个观察者无法成立。
- MaxAge 默认 24 小时，去重窗口默认 2 分钟；同时设置字节、consumer 数量与单运行预算。磁盘满拒绝新写，不静默删除未过期历史。
- consumer 空闲回收与业务恢复期限分开；默认空闲 10 分钟可以回收传输 consumer，不能同时抹掉持久消费位置。
- KV 保存调用 manifest、消费位置和 reader 所有权；其保留期至少覆盖承诺的恢复期限。对象存储保留期还必须覆盖所有引用帧与结果。
- manifest 保存资源创建身份、运行绑定、起点和有效期。已有运行的 subject 为空时，不得把已过期历史当成从未写过；缺少证明返回 DataLoss/FailedPrecondition。
- 新流与恢复均校验期限。长任务需要显式增加保留预算；不断追加新消息不等于最初输入仍在。

### 4.3 帧和 codec

复用 FrameCodec 的注册表、限额、超时和冻结字节机制。把其依赖具体 LogFrame 的部分拆成同包内的“有界字节编解码＋帧序列化”，不新增公开 codec 包。

```proto
// 结构草案；具体字段编号在 P0 固定。
message EncodedFrame {
  uint32 format_version = 1;
  repeated string codec_ids = 2;
  bytes payload = 3;
}

message StreamRecord {
  uint32 version = 1;
  string run_id = 2;
  string task_run_id = 3;
  string method = 4;
  string binding_digest = 5;
  Direction direction = 6;
  Kind kind = 7;
  int32 epoch = 8;
  string writer_nonce = 9;
  uint64 record_seq = 10;
  uint64 previous_stream_seq = 11;
  uint64 first_data_seq = 12;
  uint64 last_data_seq = 13;
  repeated bytes messages = 14; // 每条为 ProtoJSON，保留边界。
  uint64 total_data_count = 15;
  bytes content_digest = 16;
  uint64 input_generation = 17; // 输入接管使用；输出执行代次使用 epoch。
  // 按种类携带 metadata、headers、trailers、status、恢复索引。
}
```

种类包括 INPUT_OPEN、INPUT_END、OWNER、HEADERS、DATA、ATTEMPT_END。完整 StreamRecord 经过 codec；只把格式和有序 CodecID 放在外层。控制字段不单独放入可绕过解码的 NATS header。业务 DATA 不再执行第二遍 codec。

- CodecID 使用组件常量；允许重复步骤，不使用逗号拼接。
- 未知 ID 在任何下载/解密前失败；AES 继续使用随机 nonce 和 nil AAD。
- 外层 codec 列表用于解码引导，不额外宣称其具有认证能力。解码后校验完整记录和实际 subject/运行绑定一致。
- `previous_stream_seq` 连接同一 subject 的前一个持久记录，可用于有限的缺口修复；不把全局 StreamSeq 差值当作丢消息数。
- 每条记录保存累计 DATA 数及可定位的首次 headers/最新 DATA 位置；查询尾记录可取得恢复摘要，必要时再 GetMessage 读取目标记录。
- 控制帧、头部、索引、批次数量与状态 details 均计入限额，不能只限制业务消息。

## 五、有序发布与执行门禁

### 5.1 每个方向一个有效写者

每个 writer 串行发布，设置 expected_last_subject_sequence。PubAck 后才推进已确认位置。

message_id 包含调用身份、方向、epoch、writer_nonce 和该记录身份。不同 attempt/不同内容不得复用同一 message_id。发布重试固定同一 ID、期望序号和完整编码字节，不重新加密或上传。

结果不明确时：

1. 在发布预算内重试原请求。
2. 仅当结构化 NATS 错误码确认为 WrongLastSequence 时进入竞争处理；其他 FailedPrecondition 不能都解释为失去写权。
3. 读回尾记录，核对 writer、记录 ID、范围和内容摘要；不是只比较 LastSeq。
4. 自己的记录被下一代 OWNER 覆盖到尾部之后，可沿有界 predecessor 链或已知物理位置核实；无法证明成功时返回明确的不确定结果，不把它认作成功，也不换 ID 重发。
5. 一旦确认其他写者接管，旧 writer 不得刷新期望序号后继续业务发布。

这保证条件发布后的有效写入顺序，不保证外部业务副作用只发生一次。

### 5.2 Worker 接管规则

接管必须在 handler 和 STARTED/FAILED/COMPLETED 上报之前完成：

- 首次执行以条件发布竞争 OWNER。
- 相同 epoch、不同 nonce：只认第一个成功 OWNER；落选者不启动 handler，也不报告失败。
- 更低 epoch：落选，不允许重新接管。
- 更高 epoch：读取并验证当前尾部，以条件发布加入 OWNER。CAS 竞争失败重新判断代次，不能无条件覆盖。
- 旧 writer 的 DATA/ATTEMPT_END 使用其已确认前驱，OWNER 写入后会使其后续条件发布失败；取消本地 handler，抑制旧执行的终态上报。
- 不等待旧 handler 必然响应取消；它的外部副作用需要业务幂等保护。

保留并改造 Definition.BeforeStart、Admission、gatedRunner 和 CANCEL 的准确代次匹配。不能用“返回 NonRetryable”代替静默回收落选执行。

Extra 写入与 Hatchet 结果上报没有原子事务。P0 必须验证真实引擎对于旧 RetryCount 的迟到上报行为；上报前的本地检查不能消除检查后发生接管的竞态。未证明不会覆盖新代次前，不宣称实现了端到端执行 fencing。

同一探针必须覆盖 Worker 失联后的实际重派是否增加 RetryCount。不能把“同代次重复投递”和“原 writer 已死亡后的合法重派”混为一谈：若后者仍使用相同代次，仅靠首个 OWNER 获胜会永久阻塞恢复。出现这种情况，P0 必须先确定可核验的新执行身份与接管规则，同时证明旧结果上报隔离；不得单纯放开同代次抢占或假定重试配置能补救。

### 5.3 Send、缓冲与批量

- 默认 SendMsg 在对应记录获得发布确认后返回，维持明确的持久发送语义；一个方向不支持并发 Send。
- 可选的有界缓冲发送作为独立优化：Send 成功只表示进入 SDK 队列；CloseSend、显式 Flush 和 Worker 最终完成必须等待全部已接受记录发布成功。该选项必须在文档说明崩溃窗口。
- 批量仅合并相同方向、writer 和连续 DATA，不混入 OWNER/HEADERS/END。默认每条 DATA 一条存储消息，先完成正确性验收，再启用合批。
- 同步 Send 的单生产者不能凭空获得合批收益；性能报告分别测同步与显式缓冲，不混报。
- 最多一个发布请求在途；状态锁不跨 codec、Publish 或 GetMessage。发送失败进入流终态，业务忽略 Send 错误也不能上报完整成功。

## 六、消费、ACK 与位置持久化

### 6.1 三种位置不能混用

1. fetched：已经预取进 SDK。
2. delivered：成功解码并复制到调用方提供的消息对象。
3. committed：调用方选择确认的连续处理前缀，已持久保存。

默认 gRPC 接口采用交付确认：delivered 可以批量推进 committed，但不表示业务处理完成。进程在 Recv 后、业务处理前崩溃，框架不能证明处理是否发生。

需要 durable Execute、资产下发或严格处理确认时，必须选择手动确认；业务先保存处理状态，再 Commit。整个系统提供可恢复、至少一次处理的基础，不宣称业务 exactly-once。

### 6.2 consumer 与有序读取

- RPC 内部使用 AckExplicit。只有整个存储消息内的 DATA 已纳入 committed 前缀，才确认该消息。
- 预取按消息数与字节双预算控制；向 Extra 发起一次 Fetch 时选择一个服务端 batch 维度，并继续在本地检查另一个维度。
- 首次投递、重投和重连统一经过 RecordSeq/DataSeq 校验，重复只在已确认或已交付前缀中去重。
- `first <= expected <= last` 的批次从中间偏移继续；不能把批次重叠当作缺口，也不能跳过未交付部分。
- 缺口先在有界重排窗口等待/按 predecessor 链和 GetMessage 修复。临时网络失败是 Unavailable；已证明缺失/过期才是 DataLoss。
- 确需重建 consumer 时使用新的实例名；停止旧 Fetch、等待旧读取退出、建立新位置，然后删除旧资源。禁止同名删除重建后继续接收旧响应。
- 每条长期 Fetch 有预算和取消；空批次、Fetch EOF、heartbeat timeout 均不能直接变成 RPC EOF。
- 控制记录只有在其状态已保存、且此前的连续业务前缀满足确认规则后才能确认。慢消费者受 MaxAckPending 和本地字节窗口限制；必要的 IN_PROGRESS 只延长投递期限，不推进 committed。

### 6.3 持久消费状态和 ACK 顺序

Extra KV 保存以下版本化状态：运行绑定、逻辑 consumer_id、reader_generation、已提交物理位置、批次内偏移、累计 DATA 数、headers 状态以及持有者租期。

- 一个逻辑 consumer 只允许一个活跃读取所有者，使用 KV CAS 和有期限的租约接管；同一运行可用不同 consumer_id 独立回放。
- 租约到期时间保存在状态值中，续期通过 CAS 更新；不能依赖当前 Put/Update 尚未提供的单 key TTL 续期。KV 条目的存储保留期独立管理，不随短租约到期删除业务进度。
- 每次 reader_generation 使用不同的底层 consumer 名；旧实例的 ACK 不得推进新实例的读取位置。
- Commit 先 CAS 保存完整连续前缀，再对已经完整处理的批次 DoubleAck。CAS 失败说明所有权变化，不能继续确认。
- CAS 成功、ACK 丢失时，恢复按 KV 前缀过滤重投；ACK 成功前绝不能先丢掉唯一的业务进度。
- KV 持久位置是业务恢复依据，consumer ACK floor 是传输进度及校验依据；两者不能伪装成同一个原子更新。
- 自动确认可按数量/短时间批量提交；崩溃最多重投未提交窗口。手动确认不得在 Close、超时或业务失败时自动推进。
- consumer 空闲回收后按持久位置创建新实例；记录缺失时只能使用显式保存的可信恢复凭证，不猜测当前位置。

租约到期用于判断是否允许尝试接管，generation 与 CAS 才决定哪个读者可以提交进度。时钟偏差、进程暂停或网络分区可能导致提前接管和重复业务处理；失去所有权的实例停止续读与确认，但租约不强制终止已经离开 SDK 的业务代码。跨系统业务提交仍需稳定幂等键。

以上持久业务消费位置用于客户端输出消费。Worker 输入使用每次获准执行独立的 consumer：本次 Recv 后可以确认该 consumer 的传输投递，但下一次执行默认创建新 consumer 从输入起点重放。不能复用前一 attempt 的 ACK floor 来跳过业务尚未处理的输入；输入历史由 LimitsPolicy 保留，与 ACK 独立。

## 七、RPC 建立、输入、结束与恢复

### 7.1 建立与提交

client/bidi 的 Extra 模式在 NewStream 时提交任务；server stream 的生成代码先 NewStream、再 SendMsg(request)/CloseSend，所以它必须等单个请求可用后再提交。

所有本地参数、绑定、codec、Extra 配置与资源能力检查先于创建远端任务。协议示例：

```json
{
  "_wego": {
    "version": 7,
    "response": "wego",
    "idempotency_key": "operation-key",
    "stream": {
      "transport": "extra",
      "input_source": "external",
      "binding_digest": "..."
    }
  },
  "value": null,
  "routing": {"group": "group-a"}
}
```

保留必须存在的 value：外部输入采用 null，不新增“省略 value 即猜测流输入”的解析分支。直接 RPC、事件和 native 的输入形态由注册绑定与显式 input_source 校验。

- 取得 RunID 后建立输入 writer；Worker 可能更早被分配，应等待 INPUT_OPEN/输入记录，不因 subject 尚为空就判断失败。
- client/bidi 每次 Send 冻结 ProtoJSON 请求快照；CloseSend 只写一次 INPUT_END，重复调用返回同一结果；CloseAndRecv 自动调用它。
- 任务排队期间可以持久写入输入，受总量、保留期和磁盘预算约束。
- Worker Recv 按输入 DATA 逐条还原，确认 INPUT_END 计数/摘要完整后才返回 EOF。
- handler 提前返回不要求消费完输入；停止本次 feeder，通知客户端终态，剩余历史按保留期回收，不能为等待 INPUT_END 阻塞成功结束。

### 7.2 内联输入、事件与 native

server stream 是一个请求；client/bidi 内联模式是数组，空流为 `[]`。事件一份封包可以触发多个方法，幂等身份继续包含 namespace＋FullMethodName，不能混用输出日志。

保留默认 FullMethodName 事件与自定义事件去重。事件、Schedule、Cron 的发布操作 deadline 不成为未来任务 deadline。当前流定义不支持的 durable/Batch/定义级 Cron 组合不能通过此次传输重构顺便放开。

native server/bidi 仍走原生 PutStream，client stream 最终返回业务响应。wego 事件触发的 Reliable 流可以使用 Extra 保存输出，不要求一定存在实时客户端。

### 7.3 结束判定

输入结束与输出 attempt 结束是两件事。Worker 每次完成发布 ATTEMPT_END，携带 epoch、writer、结束记录身份、累计 DATA 数、完整 status、headers/trailers 和最终响应引用。

- 不通过 `RetryCount < configured retries` 猜测是否还会执行；引擎内部重投、取消和上报失败也会改变执行结果。
- Hatchet 成功输出保留 wego completion 清单，不能只改成 sent_messages。
- 成功 EOF 必须同时满足：引擎成功、清单对应的 ATTEMPT_END 匹配、输出序号/计数完整、应交付消息已交付。
- 失败使用引擎最终错误和对应清单；中间 attempt 的业务错误不提前终结正在重试的调用。
- 引擎取消/失败但没有 END 时，客户端必须被运行观察器唤醒，不能永久等帧。
- 已看到 END 而引擎尚未确认时继续等待；新 attempt 看到旧 END 也不能无条件重报成功或跳过 handler。
- 缺帧等待受恢复预算限制；传输不可用和确认的数据缺失分别报告，不能统一掩盖为 EOF。

首次已交付的 headers 不因重试替换；trailers/status 来自引擎最终接受的执行。client stream 单个响应仍由任务结果还原，必须覆盖零响应、重复响应和 SendAndClose。

### 7.4 幂等、输入续写和输出恢复

保留现有 `client.WithStreamIdempotencyKey`、`client.StreamResumeToken(ctx)` 和 `Conn.ResumeStream` 命名，扩展协议 7 凭证，不建立第二套含义相近的位置 API。

消费 token 至少包含：版本、tenant/namespace、RunID/TaskRunID、方法与绑定版本、transport、consumer_id/generation、StreamSeq、批次内偏移、DataSeq、headers 及有效期。恢复先校验持久运行绑定和响应契约，不信任 token 自报身份。

- ResumeStream 默认只恢复消费，输入关闭，取消不取消远端运行。
- 幂等冲突默认成为观察者。内联输入仍核验完整业务摘要；实时输入尚未结束，核验不可变的调用契约和 routing，不虚构一个尚不存在的完整输入摘要。
- 实时输入在每次记录中维护内容链，INPUT_END 固定最终计数和摘要；不同请求不能因为 key 相同就被当作相同输入。
- 输入续写增加独立 `StreamSendToken` 和显式 `Conn.ResumeSending`。凭证包含原输入 writer/generation、已确认位置和内容链；接管条件必须同时匹配当前输入 generation，再条件发布新的 INPUT_OPEN，将 generation 加一。并发恢复只允许一个新写者成功，旧凭证不能连续接管，INPUT_END 后不能追加输入。
- 仅知道 RunID 或幂等键，不足以自动取得输入写权。
- 发送恢复凭证只记录已确认部分；SDK 内存中未发布的数据不能凭空恢复。调用方需要可重放的数据源/持久发送日志，或者重新核对结果不明确的尾记录。
- 业务需要指定独立消费位置或手动确认时，使用专门的 gRPC CallOption，例如 `WithStreamConsumer`、`WithStreamAckMode`；它们不进入 Hatchet RunOption 调度字段。
- 提供 `client.CommitStream(ctx, token) error`，只提交当前流已交付的连续前缀，不能确认其他运行、其他 generation 或尚未交付的数据。

指定 DataSeq 回放必须有物理定位。已保存 token 可直接定位；只给业务序号时需有界扫描/稀疏索引，不能把 DataSeq 直接传给 GetMessage。首版优先提供“从头”与“从 token”回放，不承诺任意序号常数时间定位。

### 7.5 取消与调用所有权

- 新建流的原调用者主动取消或 deadline 到期，请求取消自己拥有的 Hatchet 运行，并停止本地输入/输出；取消失败可诊断，不能只关闭 Extra 连接后宣称任务已取消。
- ResumeStream、幂等冲突附着者只停止本地消费，不取得远端取消权；ResumeSending 取得输入写权也不自动取得整个任务的取消权。
- 提交结果不明确时，使用原提交键核实归属与 RunID；不能取消一个尚未证明属于本次提交的既有运行。
- durable 父执行的 eviction/替换与业务主动取消分开处理，按第八节复用子运行；临时 Fetch/Publish 断线同样不等于业务取消。
- 控制台取消由运行观察器唤醒阻塞的 Send/Recv，返回 Canceled；不能依赖 Worker 配合发布结束帧。

## 八、Worker 恢复与 durable Execute

### 8.1 Worker handler 的恢复

默认沿用业务显式恢复：`task.Checkpoint` 提供接管边界之前的有限历史；业务根据已经发布的资产/结果重建状态。新增尾部摘要读取可用于只需最后一条输出的业务，尾部读取不替代需要全部历史的恢复。

- 查询尾记录取得最后输出、headers 位置和旧 attempt 身份，无需先为确定边界扫描整条日志。
- 需要历史时只做一次按需扫描，并固定本次接管边界；不能消费自己之后发布的数据。
- client/bidi 的输入默认从头重放。Recv 次数不等于已处理输入，不自动用最后输出携带的 InputMark 跳过输入。
- 多输入合成一输出、一个输入产生多输出、并行处理、零输出处理必须单独验证。
- 可选确定性 Replay 模式先逐条比较重放输出与持久前缀，包括消息类型和内容；不足、顺序不同或内容不同明确失败，不能单纯丢弃前 L 条。
- Replay 不撤销或去重业务外部副作用，使用方仍负责幂等。

首版不提供“只返回最后一条消息，就能自动恢复任意 handler”的保证。后续若增加通用状态快照，应把业务状态、已处理输入位置和输出位置作为同一已确认记录提交；单独写 KV 再发布输出不能声称二者原子。

### 8.2 支持 Execute 调用流

这是本次必做路径，不能遗漏现有两处 durable guard。

1. Extra 模式允许 durable Execute 通过 task.Conn 创建或恢复 server/bidi/client stream；不把远程流 handler 本身改成 durable task。
2. 复用原始 durable 子调用身份、稳定 child key 和 memo 机制。memo 可保存初次子 RunID 等稳定结果，不把同一个 memo key 当作可变 cursor 存储。
3. 消费位置使用 Extra KV/逻辑 consumer；例如逻辑键由父 RunID、节点 ID 和 consumer 名组成，输入本身不作为唯一键。
4. 父执行重放时复用已经建立的子运行；结果不明确时按原幂等身份核实，不能再次提交随机 key。
5. durable eviction 或父 attempt 被替换只停止本地订阅，不取消已记录的子运行；业务主动取消整条流程使用明确的取消操作。
6. EvictionPolicy 继续由使用方配置。SDK 不擅自禁止/修改 eviction，也不能因为用户禁止 eviction 就省略进程崩溃恢复。
7. 长流不要求每次 Recv 都调用 task.Memo；后台订阅、输入发送和 child-submit 不能并发修改同一个原始 durable context。

资产扩散流程的确认顺序：

```text
接收 asset
→ 用稳定 asset ID 建立/核验下游提交记录（含幂等键）
→ 提交/附着下游任务并保存已确认 RunID
→ CAS 保存资产—RunID 映射及业务进度
→ Commit 当前连续消费位置
```

提交响应丢失、保存映射失败时不提前 Commit；重放使用稳定 key 恢复。Hatchet 的幂等 TTL 必须覆盖这段恢复窗口。一个 KV 条目可原子保存一份有界业务状态，不能把多个 key 和 Hatchet 提交描述成事务。大映射按业务分片，状态大小受限。

## 九、Stream/KV 与 Worker 通知

### 9.1 公开能力

继续采用 `conn.Stream()`、`conn.KV()` 的管理客户端风格，公开请求、返回值、迭代器和错误全部为 wego 自有类型。

- Stream 提供 stream/consumer 管理、Publish、GetMessage、Fetch/消费及显式 ACK。复用 Extra 已支持的能力，不把全部 NATS 配置对象直接透出。
- Fetch 必须保留“已经收到的消息＋后续错误”，不能因为批次中途失败而丢弃已收到的 Delivery。
- Delivery 的 ACK token 不转换为可构造的序号 ACK；ACK/DoubleAck/NAK/IN_PROGRESS/TERM 的差异保留。
- 自动 Consume 回调需要明确的 ACK/重试策略，不能对所有错误无限立即 NAK。
- KV 提供桶管理、Get/Put/Create/Update/Delete、有限 History 和 Watch。CAS 冲突、initialized、TTL、缺失值与空值保持现有 Extra 语义。
- KV 的单 key TTL 目前是 Create 选项，不能宣称 Put/Update 都能任意刷新它。

业务 Stream/KV 的 codec 值始终使用明确的版本化自描述封包，包括空 codec 链。若开放原始字节模式，必须由方法/配置显式选择；不能靠一个魔数字节猜测原始数据是否已编码。所有 I/O 纳入 Engine。

没有 Extra 配置时，KV 按已确认约定使用实例级有界内存实现，明确暴露存储类型；进程退出丢失，不用于持久恢复。支持的 Create/CAS/TTL/History/Watch 语义须分别测试，不能用无界 map 冒充完整实现。Extra 连接失败不切换内存。通用 Stream 持久管理 API 没有配置时返回明确错误，PutStream 降级仅适用于任务流出口。

### 9.2 Worker 通知

保留 Workers().SendEvent/BroadcastEvent 与 WithWorkerEventHandler，迁移到 Extra subject 和独立消费者，不占业务 slots。

- 仅在显式启用通知能力时初始化；普通 unary Worker 不再被通知通道强制依赖 Durable Streams。
- 每个 Worker key 独立定向地址；广播每个 Worker 独立 consumer，不能多个 Worker 竞争同一 consumer 导致广播变成负载均衡。
- 新 Worker 从加入位置开始，不执行加入之前的旧广播；同一实例断线可恢复。
- 旧 writer 的定向取消可以继续作为加速退出手段，正确性依赖条件发布和执行门禁。
- 内部取消与业务慢回调分开排队，队列有界，过期/重复通知按既有规则处理。
- 未配置 Extra 且未启用通知时不建立任何通知订阅；明确调用通知 API 时返回不支持，不静默丢弃。

## 十、模块、连接与并发关闭

### 10.1 模块收敛

| 位置 | 职责 |
|---|---|
| runtime / internal/spec | Extra 配置、模式选择、预算与快照 |
| client / internal/rpc | 标准 gRPC 适配、消费/发送凭证、取消所有权 |
| internal/stream | 替换旧传输实现，保留有序日志、writer/reader、门禁和终态不变量 |
| internal/wire | 协议 7 记录、Envelope、统一 codec 与错误 |
| internal/ports | 自有 transport/存储/执行接口 |
| internal/backend | Hatchet 与 Extra 协议适配、鉴权和连接 |
| internal/features / model | 自有 Stream、KV、通知客户端及数据类型 |
| internal/engine | I/O 登记、排空、连接所有权和观测 |

不同时保留 stream、streamlog、extra 三套平行状态机。无需只转发一层的包。RPC、通用 Stream/KV 共用同一个 Extra transport。

### 10.2 可发行依赖

Go internal 导入规则允许当前 wego 路径访问根仓库，但官方 v0.110.5 不包含本地 extra，这仍然不能作为发行依赖。

首版采用：以根仓库 Extra proto 为唯一契约源，为 SDK 生成私有客户端桩，放在 `internal/backend/extraapi`，只含协议数据与 gRPC 客户端，不复制 Extra/NATS 服务实现。生成脚本固定工具版本、记录契约摘要并验证 wire schema 一致。

- SDK 普通测试使用自有 transport fake 或 SDK 私有桩的受控服务。
- 真实 Extra 作为独立进程/容器测试，避免同进程同时注册两个相同 protobuf full name 的生成包。
- SDK 不为运行时引入 nats-server；只有 Extra 服务需要它。
- `GOWORK=off go test/build/vet` 始终是门禁，不改成 Skip。go.work 仅供开发联调。
- 后续若独立发布公共契约模块，可替换生成桩来源，公开 API 不变。
- Extra 的 NatsError details 在 backend 转为 wego 自有错误信息，不从错误链或动态 status details 透出额外实现类型。

### 10.3 生命周期

关闭顺序：停止新调用/任务分配 → 排空 handler 与允许完成的发送 → 确认结束清单、已承诺的 Commit/ACK 和任务结果 → 停止 Fetch/Watch/通知 → flush telemetry → 关闭 Extra 和 Hatchet 连接。

- 所有 Publish、codec、GetMessage、KV、ACK、Fetch 和运行观察器登记 SDK I/O。
- 先发布停止标记，再等待现有 goroutine；不能在 Wait 开始后新增后台登记。
- 任何状态锁不跨远程 I/O或业务回调；Send/Recv 各自单方向串行，允许同时一发一收。
- Stop 可中断 GracefulStop，释放满队列上的发送者、读取者和租约续期；清理使用独立预算。
- 强制关闭不能伪造成功 END、确认未处理消息或删除仍被运行引用的历史。
- 租约、consumer 和共享 stream 的生命周期分开。每次 Conn.Close 不删除共享 stream/bucket。
- OnFinish 终态后只调用一次，并在锁外执行；借用连接拒绝关闭共享资源。

## 十一、性能与资源预算

默认继续使用单条业务消息 1 MiB、预取 64 条/4 MiB等现有预算，并增加 Extra 编码消息预算、每流累计输入预算、存储保留期及 consumer/lease 总数上限。

- 业务消息、完整 protobuf 记录、codec 中间值、Extra 请求分别计费；S3 卸载不能绕过业务解码上限。
- Extra 默认消息体上限 1 MiB；协议开销意味着正好 1 MiB 的业务消息未必能直接发送。超限必须说明对应预算，可调整服务限额或按既有 codec 卸载。
- 队列按字节和条数限制；禁止无限预取、为每个历史消息创建 goroutine或先无限下载再检查长度。
- 单个 bidi 至少涉及输入和输出两类活跃 Fetch。实例本地 MaxReaders 不能超过服务端总预算后仍宣称可用；还需为通知和业务 Stream 预留容量。
- 当前 Extra 是单节点，不能直接启动两个独立实例并随机负载均衡，它们不会共享日志/KV。扩容先调整单节点预算；集群部署单独设计。
- 尾部恢复应为少量读取；历史恢复一次有界扫描。不能为了做到一次扫描就取消身份和完整性校验。
- 指标记录发布/确认耗时、队列字节、重投、CAS 冲突、恢复读取量与资源数，不用任意 RunID/consumer_id 作指标标签。

## 十二、实施阶段与门禁

| 阶段 | 交付 | 退出条件 |
|---|---|---|
| P0 契约与探针 | 固定协议、独立桩、create_only、CAS/ACK/重投与旧结果上报探针 | 真实 Extra 与 Hatchet 能证明关键所有权和终态规则；发行模式可编译 |
| P1 传输与资源 | 实例连接、租户/namespace、资源初始化、codec、KV 状态、生命周期登记 | 无 RPC 重构也能验证发布/消费/确认/重启；非法输入无外部 I/O |
| P2 server stream | 有序输出、门禁、consumer 进度、终态清单、手动确认和恢复 | 扩散输出可恢复，旧写者和旧 reader 被隔离 |
| P3 client/bidi | 实时输入、半关闭、提前返回、显式发送恢复、输入总预算 | CloseSend 前可交互；client stream 单响应；双向断连恢复 |
| P4 Execute 与公开能力 | durable 子身份、KV 映射、Stream/KV API、Worker 通知迁移 | 父/子进程崩溃及重放矩阵通过，无误取消子运行 |
| P5 删除与交付 | 清理旧 Durable Streams 传输、文档、性能与完整回归 | 旧能力迁移完毕，官方引擎＋Extra 验收及独立模块门禁通过 |

旧 backend Durable Streams 发布/订阅、JOIN 扫描和旧 producer 实现，在所有调用者迁移后删除。门禁、完成清单、Checkpoint、通知 API、PutStream 和 task.StreamEvent 按职责保留或改造，不能用全仓关键字必须为零作为验收标准。

协议 6 的持久流先排空；协议 7 两端协调升级。旧历史资源保留，不自动删除。暂不增加读取旧流日志的兼容状态机；迁移文档明确旧 token 不可用于新流。

## 十三、必须通过的验收

| 分类 | 必测条件 |
|---|---|
| 发布 | 响应丢失；去重窗口内/外；CAS 冲突；相同范围不同内容；尾记录已被接管记录推进；序号溢出 |
| 执行门禁 | 同代次跨进程竞争与失联重派的区别；高低代次交错；旧 handler 不响应取消；旧 END/终态迟到；落选者无 STARTED/FAILED/COMPLETED |
| ACK | ACK 丢失；KV CAS 成功而 ACK 失败；部分批次提交；重复 Commit；旧 generation ACK；提交尚未交付位置被拒绝 |
| Reader | 重投乱序；批次重叠；缺口修复；Fetch EOF；consumer 删除重建；逻辑消费位置过期；两观察者独立进度 |
| 三种流 | 零输入/零输出；实时 bidi；CloseSend 幂等；SendAndClose；handler 提前返回；慢读；满队列；headers/trailers/status details |
| 终态 | END 早于引擎结果；结果早于 END；失败 attempt 后重试；引擎取消且无 END；成功缺帧不能 EOF |
| Worker 恢复 | 历史一次扫描；尾摘要；多输入一输出/一输入多输出/零输出；Replay 内容差异必须报错 |
| 客户端恢复 | 自动/手动确认；消费 token；输入发送 token；旧发送凭证不能再次接管；INPUT_END 后不能续写；只有幂等键不能续写；未确认输入重放；恢复取消不取消运行 |
| Execute | 父 Worker 崩溃；子提交结果不明确；资产映射保存失败；Commit 前后崩溃；eviction/replay；RunID 和稳定 child key 复用 |
| codec | gzip/zstd/AES/S3及组合；不同注册顺序；阈值跳过；未知 ID；超限；取消；完整控制帧变换；冻结重试字节 |
| 降级/原生 | 未配置 Extra；显式禁用；Extra 故障不降级；native Console/事件；纯网络；task.StreamEvent；unary 无持久流依赖 |
| 公共能力 | Stream ACK 各种方式；部分 Fetch 结果＋错误；KV CAS/TTL/Watch/History；内存 KV 生命周期；广播一对多 |
| 资源与隔离 | 租户、namespace；错误资源配置；并发首次创建；保留期；磁盘满；签名密钥轮换；同名 consumer 竞争 |
| 关闭 | 启动失败；并发 Stop/GracefulStop；发布/codec/ACK 中取消；满队列；SDK I/O 清零；不响应取消的业务不阻塞永久关闭 |

测试使用就绪屏障、可注入传输故障和 deadline，不能用固定 sleep 证明正确性。P0 探针失败不能通过减少断言或跳过必需场景交付。

性能比较覆盖小消息、1 KiB、64 KiB、1 MiB，以及 1/10/100 路并发；分别报告同步确认和显式缓冲的 P50/P95/P99、吞吐、分配、RPC 次数、存储增长和恢复读取量。S3 网络测试单列，不能掩盖本地 codec/协议开销。

使用已经部署的官方 Hatchet＋PostgreSQL MQ＋Extra＋MinIO。每轮独立 namespace，只清理本轮 stream、consumer、bucket、workflow 和运行资源。所有启动点设置 `SERVER_SECURITY_CHECK_ENABLED=false`。记录实际镜像、协议摘要、RunID、关键序号、断言与清理结果，不记录凭证。

SDK 交付门禁包括 GOWORK=off 的 build/test/vet/race、API 隔离、生成复现、中文注释和 lint；运行原有 standalone、durable、Batch、DAG、事件、原生模式及 embedded 回归。手动 feature 测试不混入无需服务的单元门禁。

交付时升级至 SDK 0.5.0、协议 7，同步 changelog、版本清单、manifest、module.md、简洁的 worker.md 和迁移说明。完成标准是：实时双向流、正确的执行/消费所有权、可核验的恢复与终态，以及原有模式无回归；不能以“换成 NATS 后测试能跑通”代替这些保证。
