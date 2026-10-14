# hatchet-extra：NATS 功能清单与 gRPC 接口候选

状态：**选型材料。已采纳的 24 个 RPC 实现在 internal/services/extra，入口为 cmd/hatchet-extra。** 本目录保留全部候选供后续取舍，实际首版协议以 internal/services/extra/proto/v1/extra.proto 为准。

核验日期：2026-10-14。基线是本仓库使用的 nats.go v1.54.0，官方发布页也将其标记为最新正式版。现代 JetStream 和旧 JetStream API 都纳入清单。[版本来源](https://github.com/nats-io/nats.go/releases/tag/v1.54.0)

## 1. 先看建议

建议第一版以 **JetStream 持久消息 + durable pull consumer + 手动 ACK + KV** 为主体。它们直接解决流输出持久化、消费位置、资产到子运行的映射保存问题。

| 能力 | 首版建议 | 原因 |
|---|---|---|
| Stream 创建、查询、列表、删除、按序号读取 | 开放 | 消息容器、恢复定位和资源清理 |
| 同步发布、MsgID 去重、发布确认 | 开放 | 知道消息是否已被 JetStream 接受 |
| 异步流水线发布 | 建议开放 | 减少每条消息等待一次 RTT；逐条确认，非原子批量 |
| durable pull consumer、Fetch、状态查询 | 开放 | 服务端保存消费者投递与确认状态 |
| ACK/DoubleAck、NAK/延迟 NAK、InProgress、TERM | 开放 | 保留 NATS 的确认、重投和延期能力 |
| KV 读写、Create、Update/CAS、删除、TTL、Watch、History | 开放 | 保存恢复状态，支持并发更新冲突检测 |
| 连续 Consume RPC | 可后置 | 首版 SDK 可循环批量 Fetch，已经能够连续消费 |
| Push consumer、OrderedConsumer、Pause/Reset/Unpin | 后置 | 不是当前恢复链路的必要条件；OrderedConsumer 不替代 durable ACK consumer |
| Core NATS pub/sub、queue group、request/reply | 后置 | 有价值，但当前目标主要依赖持久消息 |
| Object Store | 后置 | 已有 MinIO/S3；如希望统一存储入口，再开放 |
| micro 服务框架 | 后置 | 已有 gRPC/任务执行入口，当前不需要再承载一套远程回调服务 |
| 连接、TLS、重连、日志回调、序列化和本地工具 | 不做业务 RPC | 属于网关配置或 SDK 本地行为 |

“开放”也是建议，并非已经决定。可以按下文单独选择每个 RPC。

## 2. 文件怎么读

| 文件 | 内容 |
|---|---|
| [method-mapping.md](method-mapping.md) | 每个原生函数/方法的准确签名、对应 RPC/字段、本地保留理由及源码链接 |
| [inventory.csv](inventory.csv) | 同一清单的表格版，方便筛选和增加“首版采用”列 |
| [inventory.json](inventory.json) | 可供程序检查、后续生成或评审工具读取的清单 |
| [nats_proxy.proto](nats_proxy.proto) | 7 个 service、71 个候选 RPC，以及每个请求/响应定义 |
| [nats_models.proto](nats_models.proto) | 38 个原生配置/信息结构的 protobuf 对照，包括 Stream/Consumer/KV/Object 配置 |
| [rpc-selection.md](rpc-selection.md) | 71 个候选 RPC 的紧凑索引，直接用于取舍 |

清单覆盖 **800 个公开函数/方法声明、22 项类型式 option、14 项接口嵌入**。包括 nats、jetstream、micro、encoders/builtin、encoders/protobuf、util、bench。这是源码声明数量，不是要实现 836 个 RPC。

以下差异已显式处理：

- Publish 与 PublishMsg 共用一个 RPC；后者只是允许额外 headers。
- callback、channel、同步迭代器共用消息传输接口，回调在业务进程运行。
- PutString / PutBytes 共用字节接口；PutFile 由客户端读文件后流式上传。
- CachedInfo、字段 getter、PubAckFuture 的等待函数不额外调用服务器。
- WithMsgID 等配置函数映射为请求字段，不能给每个选项创建一个 RPC。
- PullMaxMessages(100) 等类型转换式选项也有记录，没有因为不是函数声明而遗漏。
- 旧 JetStream 的便利订阅可能组合“查询/创建 consumer + 消费”，不强行一对一套 RPC。
- io.ReadCloser 的继承对应下载流的读取与取消；error 的继承对应本地错误呈现。
- 示例程序、测试、内部包和 nats-server 管理 API 不属于 nats.go 公共客户端方法。
- nats_iter.go 的方法按源码列入；实际编译仍受文件 build tag 与 Go 版本约束。

## 3. 所有远程能力分组

### Core NATS

原生能力：发布、带 headers 发布、请求/应答、回复、普通订阅、queue group 订阅、自动取消、排空、订阅缓冲与丢弃统计。

候选接口：

    service CoreService {
      rpc Publish(CorePublishRequest) returns (google.protobuf.Empty);
      rpc Request(CoreRequest) returns (Message);
      rpc Reply(CoreReplyRequest) returns (google.protobuf.Empty);
      rpc Subscribe(stream SubscriptionCommand) returns (stream SubscriptionEvent);
      rpc GetConnectionInfo(Scope) returns (ConnectionInfo);
    }

Subscribe 的首帧打开订阅，后续帧可以停止、排空或修改限制。接收端返回消息、就绪、统计及错误。Core NATS 不因此获得持久化能力。

### JetStream 管理与发布

原生能力：Stream 创建/更新/查询/删除、purge、按序号或 subject 读消息、擦除消息、账户状态；同步和异步发布，发布去重、期望序号检查、消息 TTL 与定时发布。

对应 StreamService 与 PublisherService。发布选项明确表示为：

    message PublishOptions {
      optional string message_id = 1;
      optional string expected_stream = 2;
      optional string expected_last_message_id = 3;
      optional uint64 expected_last_sequence = 4;
      optional uint64 expected_last_subject_sequence = 5;
      optional string expected_last_subject = 6;
      optional int64 message_ttl_ns = 7;
      optional uint32 retry_attempts = 8;
      optional int64 retry_wait_ns = 9;
      optional int64 stall_wait_ns = 10;
      ScheduleOptions schedule = 11;
    }

ScheduleOptions 单独列出一次性时间、cron、周期、目标 subject、来源 subject、时区、rollup 与输出 TTL。它们需要 NATS 对应功能开关，不因为 protobuf 存在字段就假定服务端支持。

PublishAsync 是有 correlation_id 的双向流：每条返回自己的 PubAck 或错误。关闭发送方向后等待所有已接收请求的结果。断线未确认项保留“不明确”状态，不能把它们全部判为未写入。

StreamConfig.AllowAtomicPublish/AllowBatchPublish 是服务端能力配置；nats.go v1.54.0 的 Publisher 接口没有同名的原子 PublishBatch 方法。本候选没有把普通流水线虚称为原子批量。

### JetStream 消费与 ACK

原生能力：pull/push consumer 管理、持久消费者、消费起点、重投和 ACK 策略、批量拉取、连续消费、有序临时消费、暂停、恢复、重置及优先级组。

核心映射：

| 原生方法 | gRPC 对应 |
|---|---|
| Fetch(n) / FetchBytes(n) / FetchNoWait(n) / Next() | ConsumerService.Fetch，通过数量、字节数和 no_wait 区分 |
| Consume(callback) / Messages() | ConsumerService.Consume；callback/iterator 在客户端实现 |
| Ack() | Ack(kind=ACK, confirm=false) |
| DoubleAck(ctx) / 旧 AckSync() | Ack(kind=ACK, confirm=true) |
| Nak() / NakWithDelay(d) | Ack(kind=NAK, nak_delay_ns=d) |
| InProgress() | Ack(kind=IN_PROGRESS) |
| Term() / TermWithReason(reason) | Ack(kind=TERM, term_reason=reason) |
| Info() | GetConsumer |
| CachedInfo() | 本地信息快照 |

ACK 使用服务端签发的 ack_token 保留原生投递身份，不能只传消息 index 然后拼接一个 ACK。确认的是消息处理进度，不是任意业务数据库事务。建议业务使用 AckExplicit；AckAll 会隐式确认之前的投递，不适合随意乱序完成的处理流程。

消费状态直接来自 ConsumerInfo，包括 delivered、ack floor、待 ACK、待投递、重投数量。AckFloor 不是每条业务都严格连续完成的完整映射；资产和子 RunID 关系仍由业务保存至 KV。

### KV

原生能力：

- bucket 创建、更新、查询、列表、删除；
- Put、Create、Update、Get、GetRevision；
- Delete、Purge、PurgeDeletes；
- Keys/ListKeys/ListKeysFiltered、History；
- Watch/WatchAll/WatchFiltered；
- bucket TTL、单 key 创建 TTL、删除标记保留、压缩、history、大小限制及副本配置。

特别保留：

    message KeyValueCreateRequest {
      KeyRef key = 1;
      bytes value = 2;
      optional int64 ttl_ns = 3;
    }
    message KeyValueUpdateRequest {
      KeyRef key = 1;
      bytes value = 2;
      uint64 expected_revision = 3;
    }

Create 是仅不存在时写入，Update 是按 revision 做 CAS；两者不能退化为普通 Put。该版本 KeyTTL 是 Create 选项，需要 bucket 启用 LimitMarkerTTL；不编造 Put/Update 可以任意刷新 key TTL 的原生方法。

Watch 返回 entry 或 initialized 标记，保留原生 watcher 的初始化结束语义。接收 gRPC 消息不等于持久保存 watch 进度；需要恢复时使用原生支持的 resume_from_revision，同时处理历史已经过期的情况。

### Object Store

原生能力：bucket 管理、上传/下载、对象信息、列表、删除、元数据更新、对象链接/bucket 链接、封存、Watch。

候选上传采用 client streaming，下载采用 server streaming，避免把整个对象放进一次 gRPC 消息。文件路径仅在客户端解释；服务端只接收内容和对象名。GetBytes/GetString 可以在客户端收集流，但必须应用大小限制。

### micro、连接与辅助工具

micro 用 NATS request/reply 承载服务。若开放，候选 MicroService.Serve 用双向流把请求转交给业务 handler；另提供 Info、Stats、Reset。Group 在客户端展开名称、subject 前缀和 queue group 配置。

micro.Config.StatsHandler 是配置里的 Go 回调，不是远程方法。若要完整代理自定义统计，需要额外增加统计请求/响应帧，当前候选只代理内置统计；首版建议不开放 micro。

连接认证、TLS、嵌入连接、断线重连、服务器发现、NKey/JWT 签名 callback 由服务端管理，或者由代理 SDK 管理自己的 gRPC 连接。**它们不等同于可远程操纵共享 NATS 连接的方法。** 完整方法表逐项说明了去向。

## 4. protobuf 候选的统一规则

1. **鉴权**：Hatchet token 通过 gRPC metadata 传递；tenant 从验证结果获取，不允许请求自行覆盖。所有 unary/streaming 入口都需要验证。
2. **逻辑资源名**：请求的 namespace、subject、stream、consumer、bucket 都在租户内解释。过滤表达式、通配符、headers 中的目标 subject、source/mirror、republish、object link 也必须同样转换与授权，不能只校验顶层名称。
3. **NATS 系统能力**：不开放任意 $SYS、$JS.API、内部 ACK/inbox 的透传。GetAccountInfo 仅在能够提供租户隔离数据时开放。
4. **持久 ACK**：token 绑定租户与原始投递身份，网关多副本/重启后仍可验证。DoubleAck 确认回复丢失时结果仍可能不明确；不得把传输错误解释为服务端没有确认。
5. **数据**：业务 payload 是 bytes；代理不解析 protobuf 业务类型，也不重复执行 wego codec。
6. **数值与时间**：sequence/revision 使用 uint64，计数保留对应范围；时间用 Timestamp，Go Duration 用带单位说明的纳秒整数。允许官方定义的负数哨兵，不能一概要求正数。
7. **可选配置**：scalar 用 optional 区分省略和显式零。NATS 配置枚举暂用原生数值并注明类型，适配层必须检查值域；它们不是可无检查透传的 JSON number。正式协议可在选定范围后改为 protobuf enum 并显式转换，当前字段号尚未冻结。
8. **错误**：普通 RPC 使用 gRPC status/details；异步发布等同一条流中的单项错误使用 NatsError。部分消息已交付后发生错误，仍保留错误，不能变成成功 EOF。
9. **更新语义**：Stream/Consumer/bucket 更新对应原生完整配置更新，不宣称字段级 PATCH。服务端能力不支持的字段应明确报错。
10. **取消与关闭**：取消订阅只停止这次消费，不自动删除 durable consumer；Close/Drain 不关闭共享 NATS 连接。已发布成功的数据不会因客户端取消回滚。
11. **缓冲与背压**：所有上传、预取、异步发布和回调队列有界。网络发送完成不能代替业务 ACK；不实现“发给客户端便自动确认”。
12. **多入口顺序**：gRPC 并发 unary 没有共享连接上的调用顺序。旧 Conn.Flush/Barrier 不能通过远程调用共享 NATS Flush 原样恢复语义；需要严格有序 session 时应单独设计并列为新能力。

## 5. 建议的最小首版组合

可先选以下 RPC，再按需求加上其他候选：

- StreamService：CreateOrUpdateStream、GetStream、ListStreams、DeleteStream、GetMessage。
- PublisherService：Publish、PublishAsync。
- ConsumerService：CreateOrUpdateConsumer、GetConsumer、ListConsumers、DeleteConsumer、Fetch、Ack。
- KeyValueService：CreateOrUpdateBucket、GetBucket、ListBuckets、DeleteBucket、Put、Create、Update、Get、Delete、Watch、History。

这组 **24 个 RPC** 已能建立消息日志，持久消费并手动确认，保存/比较更新恢复状态，以及列举清理资源。服务端把 NATS 的成功、错误和状态完整返回；wego 负责流协议与业务恢复，不在代理中再造 ACK 算法。

其他候选接口保留在 proto 中供取舍。是否启用某个字段，也可以独立于是否启用整个 RPC 决定；例如允许基本 Stream 配置，并不意味着首版必须允许跨集群 source/mirror。

## 6. 核验结果与限制

- 公共源码声明已逐项映射；没有未分类的方法。
- 所有映射中的 RPC 名称均存在于候选 proto。
- 两份 proto 已通过 protocompile v0.14.1 的语法、类型和 import 编译。
- 选型阶段未实现服务；当前首版实现与验收记录见 internal/services/extra/README.md 和 TEST_REPORT.md。
- 编译成功仅说明接口描述自洽，不能替代鉴权隔离、NATS 行为、重连与故障验收。
- 清单针对固定正式版本；没有把 main 分支或其他语言客户端的 API 混入。
