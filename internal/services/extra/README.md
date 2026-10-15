# hatchet-extra v1

hatchet-extra 通过 gRPC 提供 JetStream 持久消息、durable pull consumer 和 KV。数据由本进程内嵌的 NATS Server 保存，Hatchet 的 JWT 公钥和数据库负责租户认证。

服务实现位于本目录，命令入口位于 cmd/hatchet-extra。协议是 [proto/v1/extra.proto](proto/v1/extra.proto)，共有四个 service、28 个 RPC。公开客户端包为 github.com/hatchet-dev/hatchet/pkg/extra/v1，不依赖服务端 internal 包。

## 开放的接口

| service | RPC |
|---|---|
| StreamService（5） | CreateOrUpdateStream、GetStream、ListStreams、DeleteStream、GetMessage |
| PublisherService（3） | Publish、PublishAsync、PublishBatch |
| ConsumerService（7） | CreateOrUpdateConsumer、GetConsumer、ListConsumers、DeleteConsumer、Fetch、Ack、AckBatch |
| KeyValueService（11） | CreateOrUpdateBucket、GetBucket、ListBuckets、DeleteBucket、Put、Create、Update、Get、Delete、Watch、History |

Core NATS、Object Store、micro、push/ordered consumer、Pause/Reset/Unpin 不属于这个版本。配置里的旧 direct-get、KV mirror/source/republish、外部账户/跨 domain source 和 pinned consumer 参数会明确报错。

Stream 的同租户 mirror/source、subject filter/transform、republish 会执行租户寻址转换。业务值是原始 bytes，codec 由客户端或 wego 执行。

## 启动

需要提供：

- DATABASE_URL：现有 Hatchet PostgreSQL 的连接地址。
- SERVER_URL：与 Hatchet token 的 issuer/audience/server_url 一致。
- EXTRA_JWT_PUBLIC_KEY_FILE：现有 Hatchet 的未加密公钥 keyset，采用 Hatchet 的 base64 raw JSON 格式。只接受公钥，不加载 JWT 私钥或 master key。
- EXTRA_ACK_KEY：至少 32 个随机字节的 base64 字符串，保存在部署密钥中，重启保持一致。
- EXTRA_STORE_DIR：持久数据目录，重启复用。

Hatchet admin 初始生成密钥时，公钥文件名为 public_handle_unencrypted_ec256.key。若部署仅保存加密 keyset，可以用现有 EncryptionService 的 GetPublicJWTHandle().WriteWithNoSecrets 导出这个实例的公钥。

    GOWORK=off go build -o ./bin/hatchet-extra ./cmd/hatchet-extra
    ./bin/hatchet-extra \
      --listen 127.0.0.1:7080 \
      --store ./data/extra \
      --jwt-public-key /path/to/public_handle_unencrypted_ec256.key \
      --hatchet-url http://localhost:8080

默认监听 loopback；非 loopback 地址要求同时配置 --tls-cert 和 --tls-key。TLS 最低版本为 1.2。数据库配置复用 Hatchet loader，支持 --config 指向含 database.yaml 的目录。

本机容器构建和部署见 [Compose 说明](../../../sdks/wego/deployment/compose/README.md#hatchet-extra)。镜像定义位于 `cmd/hatchet-extra/Dockerfile`，使用多阶段构建、非 root 运行和独立持久卷。`export-public-key` 子命令仅供初始化导出公钥；`healthcheck` 子命令通过 TLS 验证无 token 的鉴权挑战，不启动数据库连接或内嵌 NATS。

应用通过 SIGINT/SIGTERM 触发关闭，默认排空预算为 30 秒。长期 Watch/Fetch 在预算到期后被强制中断，随后释放 gRPC、NATS 连接和 embedded server。关闭错误返回调用方；并发调用 Stop/Shutdown 只释放一次资源。

排空先停止接收新连接和新 RPC，已准入调用继续执行。登记由 gRPC Begin/End 事件管理，不能在 handler 返回时提前认定响应已完成；正常关闭确认最后的响应和 trailers 发出后才释放后端。Stop 或任一调用方更短的 deadline 可中断排空。重复 Serve 不接管也不关闭传入 listener，停止后不能重启。

## 客户端调用

每个 RPC 都使用以下 gRPC metadata：

    authorization: Bearer <现有 Hatchet 租户 token>

认证结果确定 tenant。请求没有 tenant_id，Scope.namespace 是租户内部的逻辑分区。namespace 可为空，非空时采用最长 64 字节的 ASCII 字母、数字、下划线、连字符。

Hatchet 鉴权使用实例私有的内存 LRU 缓存 token 记录和租户存在性，不依赖 NATS KV。默认 TTL 上限为 5 分钟，可用 `--auth-cache-ttl 10m` 或 Config.AuthCacheTTL 调整为正时长。有效期随机提前最多 20%，分散集中回源；命中不续期。同一个 token 并发未命中合并回源，命中时不查询 Hatchet 数据库。JWT 签名、issuer/audience/server_url 和 JWT 有效期仍每次本地检查，缓存期限不超过数据库 token 的有效期。

撤销 token 或删除租户最迟在缓存期限结束后阻止新 RPC；长流只在建立时鉴权。缓存只保存 token ID、tenant ID 和期限，不保存 bearer token，重启后清空。容量默认 32768 条，可用 `--auth-cache-entries` 或 Config.MaxAuthCacheEntries 配置，允许 1–1000000；满时淘汰最久未访问的条目。过期条目在读取时删除，没有后台清理 goroutine，未访问的过期条目仍受容量限制。缓存独立于 JetStream 的内存预算，不创建内部 bucket/stream。未命中回源数据库，验证失败不缓存。命令入口读取实时记录，不叠加 Hatchet repository 自身的读缓存。自定义 Authenticator 自行管理缓存。

共享回源由实例持有，单个请求取消只结束自身等待，不取消其他等待者的查询。每次回源最多 5 秒，实际 SQL 并发受 MaxConcurrentUnary 限制；关闭取消并等待已登记查询，再释放缓存。自定义鉴权器和 repository 必须遵守 context，不可用的适配器、typed nil 或空记录会明确失败。

Stream、consumer、bucket 的逻辑名称也采用上述 1–64 字节规则。subject 保留 NATS 的点分隔规则，filter 可包含星号和末尾大于号；发布 subject 不接受通配符。KV key 遵循 NATS KV 的字符规则。

gRPC 方法使用标准生成客户端：

    conn, err := grpc.NewClient(
        "127.0.0.1:7080",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    // 生产远程连接使用 credentials.NewTLS。
    if err != nil {
        return err
    }
    defer conn.Close()

    ctx := metadata.AppendToOutgoingContext(
        context.Background(),
        "authorization", "Bearer "+token,
    )
    client := extrav1.NewPublisherServiceClient(conn)
    ack, err := client.Publish(ctx, &extrav1.PublishRequest{
        Scope: &extrav1.Scope{Namespace: "processing"},
        Message: &extrav1.Message{
            Subject: "assets.discovered",
            Data: []byte("opaque business payload"),
        },
        Options: &extrav1.PublishOptions{
            MessageId: &operationID,
        },
    })

## 语义

### 发布

Publish 返回 JetStream PubAck，包括消息序号和 duplicate 标志。message_id 的去重范围与期限由原生 stream 的 duplicate window 决定。

PublishAsync 是双向流水线，每项需要 correlation_id，分别返回 ack 或 NatsError。结果顺序可以不同于提交顺序，不能按数组位置配对。相同 ID 在同时在途时被拒绝；完成后可再次使用该 ID。

异步发布同时受单流和实例共享在途预算约束。关闭发送方向后等待已接收请求的结果。断线未确认的发布属于结果不明，不会自动以新 ID 重发。它不提供原子批量写入。

PublishOptions 提供消息 ID、期望 stream/序号、TTL、重试及官方 schedule 参数。所有涉及 subject 的字段都执行租户映射。Nats- 控制 headers 不能从业务 headers 注入。

消息 ID、期望消息 ID 和 cron 表达式也执行 header 安全校验，禁止换行和控制字符绕过 typed options 注入原生协议。Header 预算按实际编码开销计算。该流水线使用有界同步发布确认，不接受原生异步 stall_wait_ns 参数。

### 原子批量发布

PublishBatch 是 client-streaming RPC。每个 PublishBatchRequest 携带相同 scope、batch_id 和具体 subject；客户端 CloseAndRecv 后，服务端才将已完整校验的记录作为原生原子批次提交。N 条输入仍保存为 N 条独立消息，全有或全无。返回 batch_id、batch_size、native_batch_id 和最后一条消息的实际 Stream Sequence。native_batch_id 与持久记录的 Nats-Batch-Id 相同，不暴露物理前缀。

仅支持 message_id、expected_stream，以及首条记录的 expected_last_subject_sequence。禁止批内重复 message_id、后续项携带条件、跨 subject/scope、TTL、调度及单条自动重试选项。原生控制头由服务端生成，业务 headers 不能注入。批次 ID 在 scope 内使用，原生 ID 经 scope 绑定，最大长度 64 字节。

首条等待空确认以验证原子能力与会话创建，后续非 commit 消息默认每 16 条等待一次空确认，其余使用同一有序连接不带 reply 发布。100 条批次默认等待 8 次回复；flow control=1 时每条等待。初始及中间的空确认不作为持久成功；只有最终确认匹配批次身份和数量才返回成功。尾序号冲突返回 FailedPrecondition；本地缓冲或条数上限返回 ResourceExhausted。取消接收或非法后续项不会提交前缀。提交阶段断线/超时仍可能结果未知，服务端不自动重试或重新分配业务 ID，调用方须读回核对。

需要从未知结果继续的调用方，应为冻结批次的每条记录提供稳定且不可复用的 message_id，首条携带固定的 expected_last_subject_sequence=E。每次网络重试使用新的 batch_id，全部 message_id、字节、条数和 E 保持不变。原生 Nats-Batch-Id 包含服务端作用域绑定，不能在客户端复算；它标识一次传输会话，而非跨重试的逻辑批次。

在保留期限内、禁止删除/Purge/淘汰的受管 Stream 上，可用 GetMessage(sequence=E+1, subject=精确 subject) 读取 E 之后该 subject 的第一条消息。首条 Nats-Msg-Id 匹配且原子批次头有效时，整批已经提交；当前固定 nats-server 版本的批内序号连续，批尾为首条实际序号加条数减一。读到其他消息 ID 表示竞争失败。明确的消息不存在（NatsError.nats_error_code=10037）只表示当前未观察到提交，可保持 E 并换新 batch_id 重试；不表示旧会话已放弃。所有尝试使用相同 CAS，所以旧 commit 即使迟到，也最多一个尝试提交成功。

前次结果未知后，条件冲突和批次重复 Msg-Id 错误都要先读回；不能直接视为其他生产者获胜。缺失 Stream、历史过期和查询失败不可当作未提交。此流程使用已有 RPC，不要求暂存批次查询或放弃 API。首条核对以消息 ID 永不复用且绑定同一冻结内容为前提；通用 PublishBatch 未强制提供 message_id 或 CAS，未满足此前提的调用方不具备这项安全重试合同。

例如：

```go
batch, err := publisher.PublishBatch(ctx)
if err != nil { return err }
for i, payload := range payloads {
    request := &extrav1.PublishBatchRequest{
        Scope: scope, BatchId: batchID,
        Message: &extrav1.Message{Subject: "rpc_example_client", Data: payload},
    }
    // logicalBatchID 在冻结时生成，重试保持不变；batchID 每次网络尝试重新生成。
    messageID := fmt.Sprintf("%s.%d.%d", logicalBatchID, len(payloads), i)
    request.Options = &extrav1.PublishOptions{MessageId: &messageID}
    if i == 0 { request.Options.ExpectedLastSubjectSequence = &lastSequence }
    if err := batch.Send(request); err != nil { return err }
}
ack, err := batch.CloseAndRecv()
```

### 只创建资源

CreateOrUpdateStream、CreateOrUpdateConsumer、CreateOrUpdateBucket 提供 create_only。为 true 时资源已存在就返回 AlreadyExists，即使配置相同也不修改；为 false 保留 upsert 行为。配置写入共用支持取消的准入名额，确保当前独占 embedded NATS 实例的存在性检查不被其他 upsert 穿插。调用方遇到 AlreadyExists 后自行读回验证配置。

### 消费与 ACK

消费者必须是具名 durable pull consumer。Fetch 可用 max_messages 与 max_bytes_limit 同时限制条数和字节数，同时应用服务端总预算。它使用官方 pull 协议，限制消息数和字节数，避免官方 FetchBytes 为百万条消息预分配缓冲。

Fetch.NoWait 不等待新消息；普通 Fetch 最多等待 30 秒。每条 delivery 包含消息、原生 metadata、签名 ack_token。读取成功不会自动 ACK。

| Ack.Kind | 行为 |
|---|---|
| ACK | 普通确认；confirm=true 对应 DoubleAck |
| NAK | 请求重投；可指定 nak_delay_ns |
| IN_PROGRESS | 延长原生 ACK 等待 |
| TERM | 终止这条消息的重投；可指定 term_reason |

ACK token 保存原始投递回复地址并绑定 tenant，重启继续使用同一签名密钥即可验证之前的 token。不能只用 stream sequence 构造确认地址。普通 ACK 返回只代表本地发布成功；DoubleAck 等待服务器确认。客户端超时仍可能留下结果不明确的确认。

验证签名后还核对 token 中的 stream、consumer 与原生回复地址。Fetch 按 inbox 和回复地址识别控制帧，业务 Status header 与空业务值不会被当作控制事件；接收队列有界，溢出明确报错，不静默漏交付。

ConsumerInfo 原样提供 delivered、ack floor、待确认、积压和重投状态。AckAll 会隐式确认之前的投递；独立并发处理建议使用 AckExplicit。

AckBatch 接收有界 items 数组，顺序返回 index 与可选 NatsError。每条复用 Ack 的 token 验证和 Kind/confirm 规则，非法 token 或其他单项错误不阻止后续独立确认。批量 ACK 不是事务；RPC 取消或响应丢失时，已经处理的前缀可能生效。confirm=false 仍只承诺本地提交。

独立历史扫描可创建随机名称的具名 consumer，设置 AckNone 和 InactiveThreshold，读取后 DeleteConsumer；它不会推进正常 durable consumer 的进度。临时资源仍受服务端 consumer 数量限制。

### KV

Create 仅在 key 不存在时写入；Update 使用 expected_revision 做 CAS。创建冲突返回 AlreadyExists，revision 冲突返回 Aborted。Put 是覆盖写入。

桶 history 受官方 64 个版本上限约束。单 key TTL 是 Create 选项，需要 bucket 配置 limit_marker_ttl；Put/Update 不提供刷新该 TTL 的接口。

Get 可读取最新值或指定 revision。空 bytes 是合法值；不存在或已删除返回 NotFound。Delete 写入删除标记，History 保留有限历史。

Watch 返回 entry 和独立 initialized 标记。resume_from_revision 对应原生起始 revision，并不创建持久 ACK consumer；历史已过期时不保证能够回放被清理的变更。

## 配置预算

| 配置 / CLI flag | 默认 |
|---|---:|
| MaxMessageBytes / --max-message-bytes | 1 MiB |
| MaxFetchMessages / --max-fetch-messages | 100 |
| MaxFetchBytes / --max-fetch-bytes | 4 MiB |
| MaxPublishBatchMessages / --max-publish-batch-messages | 100，最大 1000 |
| MaxPublishBatchBytes / --max-publish-batch-bytes | 4 MiB，含头部预算 |
| MaxPublishBatchesInFlight / --max-publish-batches-inflight | 32，实例共享，最大 1024 |
| MaxAckBatchMessages / --max-ack-batch-messages | 100 |
| MaxAsyncInFlight / --max-publish-inflight | 64，实例共享 |
| MaxConcurrentUnary / --max-concurrent-unary | 128 |
| MaxConcurrentStreams / --max-concurrent-streams | 1024 |
| MaxConcurrentReads / --max-concurrent-reads | 64，只用于 Read |
| BatchCommitTimeout / --batch-commit-timeout | 8 秒 |
| BatchAbandonTimeout / --batch-abandon-timeout | 10 秒，配置原生 MaxBatchTimeout |
| PublishBatchFlowControl / --publish-batch-flow-control | 16，首条仍等待空确认 |
| --max-consumers | 请求 10000；账户总上限未强制，GetLimits 返回 0 |
| MaxStoreBytes / --max-store-bytes | 1 GiB |
| MaxMemoryBytes / --max-memory-bytes | 256 MiB |
| --shutdown-timeout | 30 秒 |
| AuthCacheTTL / --auth-cache-ttl | 5 分钟上限，过期抖动为 4–5 分钟 |
| MaxAuthCacheEntries / --auth-cache-entries | 32768 条，满时 LRU 淘汰 |

unary、长期流和 Read 三类入场预算独立。过载立即返回 ResourceExhausted，NatsError.reason 区分可重试的名额耗尽与配置超限、存储容量不足。MaxConcurrentRPCs / --max-concurrent-rpcs 兼容一个版本：Go Config 只在两个新字段均为零时用其赋值；CLI 显式旧 flag 不能与任一新 admission flag 或对应环境变量同时配置。

新增环境变量：EXTRA_MAX_CONCURRENT_UNARY、EXTRA_MAX_CONCURRENT_STREAMS、EXTRA_MAX_CONCURRENT_READS、EXTRA_BATCH_COMMIT_TIMEOUT、EXTRA_BATCH_ABANDON_TIMEOUT、EXTRA_PUBLISH_BATCH_FLOW_CONTROL、EXTRA_MAX_CONSUMERS、EXTRA_MAX_PUBLISH_BATCHES_INFLIGHT。CLI 显式值覆盖同名环境值，非法数值在数据库初始化之前失败。提交预算必须为正且小于放弃预算，并服从更短的客户端 deadline。批次字节预算至少为 MaxMessageBytes+256。

NATS 2.15.1 的原生账户 MaxConsumers 在单节点按每个 Stream 执行，不能代表账户总数。当前不映射这个字段，GetLimits.max_consumers 恒为 0，请求正上限时启动日志明确报告未强制。--max-consumers=0 表示不请求强制；Go Config 用 -1 表达相同含义。原生每 Stream 默认 1000 的限制及显式 Stream.MaxConsumers 仍适用。SDK 必须自行管理 scope 的资源预算。

当前单节点 Stream、Consumer、KV 的 Replicas>1 返回 REPLICAS_UNSUPPORTED。Stream/KV MaxBytes 不能超过相应存储介质的实例预算；-1 保留不限的原生语义。桶/stream 的 value/message 上限不能超过服务端最大值。

## 实例限额与快照读取

StreamService.GetLimits 是鉴权 unary，返回实际生效的大小、条数、入场预算、批次超时、流控和 single_node。响应不含目录、token、密钥或账户名称。publish_batch_per_message_overhead 为导出常量 PublishBatchPerMessageOverhead=256。

批次计费保持 sum(m.Size()+256)，m.Size() 包括映射后的 subject、用户及 typed 选项头、数据；256 预留原生批次头与回复地址。SDK 不能仅计算 payload。物理 subject 前缀长度可由逻辑 namespace 的 UTF-8 字节长度推导，无须知道租户值：36+max(1, ceil(8*len(namespace)/5)) 字节。头部按 NATS 编码计算，typed CAS 只计入首条，实际 native_batch_id 不需提前取得。

StreamService.Read 接受 StreamRef、精确 subject 和 start_sequence/end_sequence 闭区间。start=0 按 1；end=0 冻结当前该 subject 的尾部，显式 end 也不会超过入场快照尾部，不跟随新消息。max_messages/max_bytes 是整个 RPC 的截断预算，缺省为 Fetch 上限，超限参数拒绝；字节按 NATS 投递 m.Size() 计算，包含 header、subject、回复地址和 data，不是仅载荷。超过可交付条数/字节后正常结束，首条大于字节预算时可返回空流。Read 不解释 DATA/EOF，不保证 SDK 业务历史完整。

服务端生成随机名字的临时 AckNone Pull consumer，InactiveThreshold=30 秒兜底，RPC 结束即用独立 5 秒预算删除。取消、发送失败也清理；删除失败记录无秘密的 stream/consumer 名，不能把清理失败返回为成功。Read 不推进正常 durable consumer 的投递或 ACK 进度。下一页使用最后收到的实际 Stream Sequence+1，仍携带原快照 end；SDK 负责期限、H0、类型校验与业务解码。

NatsError.reason 的稳定值为 BATCH_CAPACITY、BATCH_MESSAGE_LIMIT、BATCH_BYTE_LIMIT、ACK_BATCH_LIMIT、MESSAGE_TOO_LARGE、RPC_CAPACITY、STREAM_CAPACITY、READ_CAPACITY、FETCH_BUFFER、STORE_EXHAUSTED、CONSUMER_LIMIT、REPLICAS_UNSUPPORTED、PUBLISH_PRECONDITION、BATCH_IN_FLIGHT。原生 api_code/nats_error_code 保留；单批超限不能按名额耗尽无限重试，条件冲突或重复消息拒绝在前次结果未知时仍须先核对。

## 模块和边界

- server.go：embedded NATS、gRPC 注册、预算和生命周期。
- auth.go / hatchet_auth.go / auth_cache.go：认证拦截器、公钥验证、有界内存缓存及 tenant 归属校验。
- address.go / config.go：逻辑寻址、嵌套配置转换和本地校验。
- streams.go / limits.go / read.go / publisher.go / batch.go / consumers.go / ack.go / kv.go：28 个 RPC。
- convert_generated.go：静态字段转换，运行时不经过 JSON/反射中转。
- proto/v1：protobuf 协议源；公开生成客户端/服务端桩位于 pkg/extra/v1。
- scripts：固定版本的代码生成与转换生成。

embedded NATS 不打开 TCP 端口，内部连接走 InProcessConn。当前为单节点持久服务，不提供集群配置或多节点容灾。长流在建立时认证，重连会重新检查 token。

本版本没有修改 wego SDK 接入层；SDK 对 Extra 的调用可以使用这里生成的 gRPC 协议继续接入。

## 验证

普通测试包含真实 embedded NATS 和 TCP gRPC，不需要外部服务，不会因能力缺失 Skip。

    GOWORK=off go test ./internal/services/extra/... ./cmd/hatchet-extra/... -count=1
    GOWORK=off go test -race ./internal/services/extra/... ./cmd/hatchet-extra/... -count=1
    GOWORK=off go vet ./internal/services/extra/... ./cmd/hatchet-extra/...
    golangci-lint run ./internal/services/extra/... ./cmd/hatchet-extra/...
    GOWORK=off go test ./internal/services/extra \
      -run '^$' -fuzz=FuzzSubjectAndACK -fuzztime=5s -parallel=2

生成工具固定为 protoc 29.6、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.5.1：

    PATH="$PWD/sdks/wego/.tools/bin:$PATH" \
      internal/services/extra/scripts/generate.sh

设置 EXTRA_GENERATE_OUTPUT 为临时目录时，转换文件写入该目录，公开桩写入该目录的 pkg/extra/v1；可复现生成并与已提交文件比较。也可用 EXTRA_GENERATE_PUBLIC_OUTPUT 单独指定公开桩目录。

命令进程的真实 Hatchet 验收需配置 DATABASE_URL、SERVER_URL、EXTRA_JWT_PUBLIC_KEY_FILE、EXTRA_ACK_KEY，以及 EXTRA_TEST_BINARY、EXTRA_TEST_HATCHET_TOKEN：

    export SERVER_SECURITY_CHECK_ENABLED=false
    GOWORK=off go test -tags=integration ./cmd/hatchet-extra \
      -run TestLiveProcess -count=1 -timeout=2m

该测试在临时存储目录启动命令进程，实际调用 28 个 RPC，清理本轮 Extra 资源并验证 SIGTERM 退出。缺少配置明确失败，不 Skip。验收记录见 [TEST_REPORT.md](TEST_REPORT.md)。
