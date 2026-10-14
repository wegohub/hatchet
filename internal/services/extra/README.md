# hatchet-extra v1

hatchet-extra 通过 gRPC 提供 JetStream 持久消息、durable pull consumer 和 KV。数据由本进程内嵌的 NATS Server 保存，Hatchet 的 JWT 公钥和数据库负责租户认证。

服务实现位于本目录，命令入口位于 cmd/hatchet-extra。协议是 [proto/v1/extra.proto](proto/v1/extra.proto)，共有四个 service、24 个 RPC。

## 开放的接口

| service | RPC |
|---|---|
| StreamService（5） | CreateOrUpdateStream、GetStream、ListStreams、DeleteStream、GetMessage |
| PublisherService（2） | Publish、PublishAsync |
| ConsumerService（6） | CreateOrUpdateConsumer、GetConsumer、ListConsumers、DeleteConsumer、Fetch、Ack |
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

共享回源由实例持有，单个请求取消只结束自身等待，不取消其他等待者的查询。每次回源最多 5 秒，实际 SQL 并发受 MaxConcurrentRPCs 限制；关闭取消并等待已登记查询，再释放缓存。自定义鉴权器和 repository 必须遵守 context，不可用的适配器、typed nil 或空记录会明确失败。

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

### 消费与 ACK

消费者必须是具名 durable pull consumer。Fetch 支持消息数或字节数上限，同时应用服务端总预算。它使用官方 pull 协议，限制消息数和字节数，避免官方 FetchBytes 为百万条消息预分配缓冲。

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

### KV

Create 仅在 key 不存在时写入；Update 使用 expected_revision 做 CAS。创建冲突返回 AlreadyExists，revision 冲突返回 Aborted。Put 是覆盖写入。

桶 history 受官方 64 个版本上限约束。单 key TTL 是 Create 选项，需要 bucket 配置 limit_marker_ttl；Put/Update 不提供刷新该 TTL 的接口。

Get 可读取最新值或指定 revision。空 bytes 是合法值；不存在或已删除返回 NotFound。Delete 写入删除标记，History 保留有限历史。

Watch 返回 entry 和独立 initialized 标记。resume_from_revision 对应原生起始 revision，并不创建持久 ACK consumer；历史已过期时不保证能够回放被清理的变更。

## 配置预算

| 配置 / CLI flag | 默认 |
|---|---:|
| MaxMessageBytes / --max-message-bytes | 1 MiB |
| MaxFetchMessages / --max-fetch-messages | 64 |
| MaxFetchBytes / --max-fetch-bytes | 4 MiB |
| MaxAsyncInFlight / --max-publish-inflight | 64，实例共享 |
| MaxConcurrentRPCs / --max-concurrent-rpcs | unary 和 streaming 各 128 |
| MaxStoreBytes / --max-store-bytes | 1 GiB |
| MaxMemoryBytes / --max-memory-bytes | 256 MiB |
| --shutdown-timeout | 30 秒 |
| AuthCacheTTL / --auth-cache-ttl | 5 分钟上限，过期抖动为 4–5 分钟 |
| MaxAuthCacheEntries / --auth-cache-entries | 32768 条，满时 LRU 淘汰 |

两个 RPC 入场预算分开，长期订阅满额不会占用 ACK/查询的预算。过载立即返回 ResourceExhausted。桶/stream 的 value/message 上限不能超过服务端最大值。

## 模块和边界

- server.go：embedded NATS、gRPC 注册、预算和生命周期。
- auth.go / hatchet_auth.go / auth_cache.go：认证拦截器、公钥验证、有界内存缓存及 tenant 归属校验。
- address.go / config.go：逻辑寻址、嵌套配置转换和本地校验。
- streams.go / publisher.go / consumers.go / ack.go / kv.go：24 个 RPC。
- convert_generated.go：静态字段转换，运行时不经过 JSON/反射中转。
- proto/v1：协议及生成客户端/服务端桩。
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

设置 EXTRA_GENERATE_OUTPUT 为临时目录可以复现生成并与已提交文件比较。

命令进程的真实 Hatchet 验收需配置 DATABASE_URL、SERVER_URL、EXTRA_JWT_PUBLIC_KEY_FILE、EXTRA_ACK_KEY，以及 EXTRA_TEST_BINARY、EXTRA_TEST_HATCHET_TOKEN：

    export SERVER_SECURITY_CHECK_ENABLED=false
    GOWORK=off go test -tags=integration ./cmd/hatchet-extra \
      -run TestLiveProcess -count=1 -timeout=2m

该测试在临时存储目录启动命令进程，实际调用 24 个 RPC，清理本轮 Extra 资源并验证 SIGTERM 退出。缺少配置明确失败，不 Skip。验收记录见 [TEST_REPORT.md](TEST_REPORT.md)。
