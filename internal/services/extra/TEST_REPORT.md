# hatchet-extra 首版验收记录

验收日期：2026-10-15（全面评审与回归）。

实现范围：四个 gRPC service、24 个 RPC；入口 cmd/hatchet-extra；服务 internal/services/extra。

## 运行基线

| 项目 | 值 |
|---|---|
| Go | 1.26.7，darwin/arm64 |
| NATS Go | v1.54.0 |
| embedded NATS Server | v2.15.1 |
| 本机 Hatchet | 官方 engine/dashboard v0.110.5 |
| 本机 PostgreSQL | 18.6 |
| Hatchet MQ | PostgreSQL |
| 数据保存 | 测试独立临时 JetStream 目录 |
| 认证 | Hatchet ES256、公钥、数据库 token 校验 |
| protobuf 工具 | protoc 29.6 / protoc-gen-go v1.36.12 / protoc-gen-go-grpc v1.5.1 |

本轮使用既有 Hatchet token，只读取 Hatchet 数据库的认证记录。报告不保存 token、数据库连接串或 ACK 密钥。

## Compose 镜像部署验收（2026-10-15）

部署到 `sdks/wego/deployment/compose/docker-compose.yml` 的同一项目，本机新增 `hatchet-extra` 和一次性 `extra-init`。现有官方 Hatchet engine/dashboard、PostgreSQL MQ 和 MinIO 保持运行。SDK 部署模板随版本更新为 0.4.27，业务消息协议仍为 6。

| 项目 | 结果 |
|---|---|
| 源码多阶段镜像 | `hatchet-extra:local`，linux/arm64，约 89 MB |
| 镜像 ID | `sha256:2aa48937c5951b062db8d23b6e7bcb3fec17fd9eb0662f58527a6efcf1e4f55b` |
| 服务地址 | `localhost:7080`，TLS，127.0.0.1 端口绑定 |
| 运行约束 | UID 10001、只读根文件系统、无 capabilities、不挂载 Hatchet 私钥 |
| 初始化 | 公钥导出成功；重复初始化复用 ACK 密钥和 TLS 证书 |
| 健康检查 | TLS + HTTP/2 + 无 token 返回 Unauthenticated，通过 |
| `TestDeployedService` | 真实 Hatchet token，24 个 RPC，通过 |
| `TestDeploymentPersistence` 写入→容器重启→读取 | 消息、KV、consumer ACK floor 和未确认数保持，通过 |
| 重启前 ACK token | 重启后 DoubleAck 成功，ACK floor 从 1 推进到 2，通过 |
| SIGTERM / 重建 | 容器正常退出并重新达到 healthy |
| 本轮资源清理 | 独立 namespace 的 stream、consumer、bucket 已删除；临时 ACK 文件已删除 |

镜像构建、Compose 配置解析、shell 语法检查、39 个普通顶层测试、race、vet、普通与 integration-tag golangci-lint 均通过。部署辅助代码的公钥/证书文件路径额外有三处带说明的 gosec 局部标注；不读取 RPC 提供的路径，不扩大服务接口或关闭检查规则。

复现已部署服务验收时提供 `EXTRA_TEST_ADDRESS`、`EXTRA_TEST_CA_FILE` 和 `EXTRA_TEST_HATCHET_TOKEN`，然后执行：

```sh
GOWORK=off go test -tags=integration ./cmd/hatchet-extra \
  -run '^TestDeployedService$' -count=1 -timeout=2m
```

持久性测试还需指定随机 `EXTRA_TEST_NAMESPACE`、临时 `EXTRA_TEST_ACK_FILE`，先以 `EXTRA_TEST_PHASE=write` 运行 `TestDeploymentPersistence`，重启服务，再以 `EXTRA_TEST_PHASE=read` 运行同一测试。两阶段使用相同 namespace 和 ACK 文件，读取阶段只清理该测试创建的资源。

## 验证命令

| 命令/门禁 | 结果 |
|---|---|
| GOWORK=off go test ./internal/services/extra/... ./cmd/hatchet-extra/... -count=1 -timeout=3m | 通过 |
| GOWORK=off go test -race ./internal/services/extra/... ./cmd/hatchet-extra/... -count=5 -timeout=2m | 通过 |
| GOWORK=off go test -race ./internal/services/extra -run 'Test(StartupAndCloseFailures\|ConcurrentCASAndShutdown)$' -count=100 -timeout=90s | 通过，16.611 秒 |
| GOWORK=off go vet ./internal/services/extra/... ./cmd/hatchet-extra/... | 通过 |
| GOWORK=off go test ./internal/services/extra -run '^$' -fuzz=FuzzSubjectAndACK -fuzztime=5s -parallel=2 | 通过，4,884 次执行 |
| 固定工具生成至临时目录，与三份生成文件逐字节比较 | 通过 |
| git diff --check | 通过 |
| GOWORK=off go build -o 临时路径/hatchet-extra ./cmd/hatchet-extra | 通过 |
| GOWORK=off go test -tags=integration ./cmd/hatchet-extra -run TestLiveProcess -v -count=1 -timeout=2m | 通过 |

真实进程测试所需的连接、token、公钥和 ACK 密钥通过进程环境/临时文件注入。启动点显式设置 SERVER_SECURITY_CHECK_ENABLED=false。

## 鉴权内存缓存专项验收

鉴权缓存采用实例私有 LRU，不创建或访问 NATS KV。默认 32768 条、TTL 上限 5 分钟，实际期限随机为 4–5 分钟；命中不续期，期限不超过数据库 token 有效期。验证失败不缓存，JWT 仍每次本地验证。撤销与租户删除在缓存到期后对新 RPC 生效。

| 本轮命令 | 结果 |
|---|---|
| GOWORK=off go test ./internal/services/extra/... ./cmd/hatchet-extra/... -count=1 -timeout=3m | 通过，3.245 秒 |
| GOWORK=off go test -race ./internal/services/extra/... ./cmd/hatchet-extra/... -count=5 -timeout=3m | 通过，14.207 秒 |
| GOWORK=off go test -race ./internal/services/extra -run 'Test(TokenMemoryCache\|TokenCache.*\|HatchetJWT\|StartupAndCloseFailures\|ConcurrentCASAndShutdown)$' -count=20 -timeout=2m | 通过，10.263 秒 |
| GOWORK=off go vet ./internal/services/extra/... ./cmd/hatchet-extra/... | 通过 |
| 重新构建 hatchet-extra，执行 TestLiveProcess 本机验收 | 通过，24 个 RPC 与 SIGTERM 退出 |

64 个并发鉴权调用仅查询一次 token 表和一次租户表。内存缓存单元测试无需启动 NATS；本机验收继续使用官方 Hatchet 和 PostgreSQL，只读认证数据，不修改既有 Hatchet 业务资源。

## 单元与真实 embedded 集成

普通门禁包含 37 个顶层测试及表驱动参数场景；真实 embedded 测试同时运行 NATS 和 TCP gRPC，并使用空闲端口。鉴权内存缓存测试不启动 NATS。必需场景没有 Skip。

| 测试 | 断言 |
|---|---|
| TestStreamAndPublisher | Stream 五个 RPC；同步发布、MsgID 去重；异步 20 项独立确认；按序号/subject 读取及逻辑名称返回 |
| TestConsumerACK | Consumer 六个 RPC；乱序 ACK、NAK 重投、InProgress、TERM、积压和确认状态 |
| TestKVOperations | bucket 管理、Create 冲突、CAS、历史 revision、空 bytes、删除及历史 |
| TestWatchResumeAndTTL | initialized 屏障、Watch 取消与 revision 恢复、单 key TTL |
| TestTenantAndNamespaceIsolation | 查询、列表、发布、KV、跨租户 ACK、损坏 ACK token、缺少认证 |
| TestPersistentRecovery | 服务整体重启后消息/KV/消费者状态恢复；使用旧投递 token 完成 DoubleAck |
| TestConcurrentCASAndShutdown | 16 个同 revision 更新仅一个成功；长期 Watch 排空及短预算强制关闭 |
| TestFetchBudgetsAndDelayedNAK | byte Fetch、延迟 NAK、TERM reason、deadline、参数和批次限制、发布前置条件 |
| TestPipelineErrorsAndAdmission | 单项错误继续返回其他项；共享在途限制；Watch 满额仍可查询 |
| TestStartupAndCloseFailures | typed nil 配置、停止先于启动、重复启动、并发 Stop |
| TestTLS | TLS unary 与 server streaming |
| TestHatchetJWT | 真正的 Hatchet 签名/验证；撤销、过期、错误签名/issuer、数据库错误及 tenant 归属 |
| TestTokenMemoryCache | 默认 5 分钟 TTL、过期抖动；64 个并发调用只读一次 token/租户；无效签名拒绝；命中不续期；实例缓存隔离 |
| TestTokenCacheExpiryAndFailures | 真实 TTL 到期回源、数据库错误不缓存、已取消请求不访问数据库 |
| TestTokenCacheValidityAndTenant | 数据库有效期限制、删除租户拒绝、验证失败不填充缓存 |
| TestTokenCacheLRU | 容量限制、LRU 淘汰、更新不增加条目、返回值快照隔离 |
| TestTokenCacheConcurrentEviction | 并发读写和淘汰后索引/LRU/容量一致 |
| TestTokenCacheCanceledWaiter | 回源等待者取消不影响另一请求；同一个 token 仅一次查询 |
| TestValidationBeforeIO | 不完整/非法请求先于后端 I/O 拒绝，大小超限明确失败 |
| TestConfigAndAddressValidation | 配置预算、namespace/name/subject 和通配符规则 |
| TestImmutableConversion | protobuf 与原生配置/响应的 map、切片独立快照 |
| TestScopedNestedConfig | source/mirror 寻址转换及禁止外部账户 |
| TestAuthHeaders | Bearer 大小写、重复/畸形 header、身份注入 |
| TestErrorMapping | 取消、deadline、NotFound、Unavailable 和 CAS 冲突 |

## 全面评审与修复

本轮逐文件检查服务、命令、协议、生成器和测试。生成文件通过生成源审查和逐字节复现验证，不在生成文件中手工改逻辑。

| 问题 | 修复与回归断言 |
|---|---|
| 鉴权共享回源使用首个请求的取消信号 | 改用实例取消与 5 秒预算；TestCacheLeaderCancellation、TestCacheShutdownCancelsLoads |
| 请求提前取消可让实际回源超出入场预算 | 增加实际查询容量与关闭登记；TestCacheLoadBudget |
| typed nil 和异常空数据库记录可能引发 panic | 构造和读取边界校验；TestAuthRepositoryNilGuards |
| 业务 Status header 被当作 pull 控制事件，空值 503 被 SDK 误转换 | 使用有界原始投递队列，按 inbox/Reply 判断控制帧；TestFetchBusinessStatusHeaders |
| typed 发布选项绕过 header 校验 | 拒绝消息 ID、期望 ID、cron 的控制字符，并计算实际 header 大小；TestPublishHeaderOptionsBeforeIO |
| 重复 Serve 可关闭正在使用的同一 listener | 拒绝时不接管资源；TestDuplicateServeKeepsListener |
| 强制停止等待 gRPC handler 屏障 | 自有 Begin/End 登记，强制路径不先进入 gRPC GracefulStop；TestShutdownDoesNotWaitForUnresponsiveAuth |
| 正常排空可能在最后响应发出前关闭传输 | 登记覆盖 gRPC 响应处理，随后完成正常传输排空；TestGracefulShutdownPreservesResponse |
| ACK 描述身份与原生地址未作一致性校验 | 签名后核对原生 metadata 和租户前缀；TestACKSignedIdentityMismatch |
| source/mirror 状态的 subject 未还原逻辑名称 | 同时转换配置和运行状态；TestSourceInfoLogicalSubjects |
| pinned 配置和空 subject token 的 KV key 未提前拒绝 | 补齐本地校验；TestKVAndConsumerValidationBeforeIO |
| 跨租户 republish 缺少真实运行断言 | TestRepublishTenantIsolation 验证相同 stream/subject 的隔离 |
| 字面量、短函数、导入与注释不规范 | 展开复合配置，补中文 API/类型/并发约束注释；执行仓库 lint 和更严格的注释检查 |

未扩大 24 个 RPC 的功能范围。协议字段编号保持不变，更新约束注释并重新生成桩。未删除或修改既有 Hatchet 业务数据。

### 本轮最终门禁

| 门禁 | 最终结果 |
|---|---|
| 全部 Extra 服务与命令普通测试，count=1 | 通过，3.254 秒 |
| 全部 Extra 服务与命令 race，count=5 | 通过，14.983 秒 |
| 正常排空、强制停止、缓存关闭与取消、重复 listener，race count=20 | 通过，7.363 秒 |
| 仓库 .golangci.yml，golangci-lint v2.14.0 | 0 issues；含 revive、staticcheck、gosec、errcheck、govet 等 |
| 额外 revive 检查：exported/package-comments/receiver-naming/redefines-builtin-id，包含测试 | 0 issues |
| go vet 与手写 Go 格式/空白检查 | 通过 |
| FuzzSubjectAndACK，5 秒、parallel=2 | 通过，14,766 次执行 |
| 固定工具在临时目录生成三份文件并逐字节比较 | 通过 |
| 重新构建命令，真实 Hatchet PostgreSQL/token 下调用 24 个 RPC，验证 SIGTERM | 通过，TestLiveProcess 1.06 秒 |

本轮仅执行 Extra 相关门禁，没有把其他服务的整个仓库测试宣称为已通过。修复与格式整理覆盖手写 Go、测试、协议注释和生成源；生成文件由工具重建。六处 gosec 数值/路径/抖动误报采用有明确约束说明的局部标注，没有关闭规则或排除业务文件。

普通 JWT 测试的数据库接口使用受控记录；真实进程测试进一步覆盖本机 PostgreSQL。

## 命令进程与本机 Hatchet

TestLiveProcess 实际启动构建后的 hatchet-extra：

1. 从既有 Hatchet 配置导出公钥验证 handle，仅公钥交给 Extra。
2. 使用现有数据库与有效租户 token，经 RPC 查询验证启动就绪。
3. 在随机 namespace 中调用并断言全部 24 个 RPC。
4. 验证 Watch 删除通知与取消。
5. 删除本轮 bucket、consumer、stream。
6. 发送 SIGTERM，断言进程正常退出。
7. 临时数据目录由测试清理。

未新建或删除 Hatchet workflow、任务运行、触发器、租户或 token。没有使用 RabbitMQ。

## 实现边界

- 本轮为单节点 embedded 服务，尚未包含集群部署。
- 服务提供 NATS 原生投递/确认和 KV 能力，不提供业务处理与 KV 的跨操作事务。
- protobuf 本地编译/复现、接口实际调用与真实进程验收都已执行。
- 未选 RPC 明确不在首版服务描述中。
- 当前没有修改 wego SDK 接入层。
