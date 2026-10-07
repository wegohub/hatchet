# wego SDK 0.1.11

wego 把标准 Go gRPC handler 和 protobuf 客户端桩接入 Hatchet standalone 任务。`client.Conn` 实现 `grpc.ClientConnInterface`，`server.Server` 实现 `grpc.ServiceRegistrar`；公开 API 使用 wego 类型。

当前简洁性处理及官方 v0.110.5 镜像验证见 [完成报告](docs/conciseness-fixes.md)。

独立代码评审的背景、功能范围、验收证据与限制见 [评审背景文档](docs/review-context.md)。Claude 评审的逐项处理见 [修复记录](docs/claude-review-fixes.md)；真实验收以报告注明的实际版本为准。

```go
cfg := []runtime.Option{
    runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
    runtime.WithAddress("localhost:7077"),
    runtime.WithServerURL("http://localhost:8080"),
    runtime.WithTLSConfig(nil),
    runtime.WithNamespace("my_app_"),
}
srv := wego.NewServer(server.WithRuntime(cfg...), server.WithGRPC(":9000"))
pb.RegisterGreeterServer(srv, &greeter{})
go func() {
    if err := srv.Serve(); err != nil { /* 应用上报错误 */ }
}()
defer srv.Stop()
// 调用携带 deadline；独立 Worker 的注册与分配由引擎协调。

conn, err := client.New(client.WithRuntime(cfg...))
// 处理 err。
defer conn.Close()
rpc := pb.NewGreeterClient(conn)
out, err := rpc.SayHello(ctx, request)
```

网络调用直接执行共享 handler；`runtime.WithDisableWorker()` 关闭 Server 的 Worker 入口，纯网络模式无需 token。原生 gRPC 配置通过 `server.WithGRPC` 透传；Worker 的任务配置保持独立。双入口独立示例：`sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/dual-entry`。

完整注册、durable、三种流和关闭示例见 [worker.md](docs/design/worker.md)，包边界和阶段验收见 [module.md](docs/design/module.md)。可运行的实现与断言在 [examples/scenarios](examples/scenarios)；[acceptance.json](examples/acceptance.json) 映射 28 个官方源文件及其辅助函数中的全部 standalone 定义。

## 本机运行

复用 PostgreSQL MQ 的 Hatchet 实例，默认 API `http://localhost:8080`、gRPC `localhost:7077`、明文。设置 `HATCHET_CLIENT_TOKEN`，或让本机运行脚本读取 `WEGO_TOKEN_FILE`（支持 JWT 文件或 `.env`；默认 `examples/go/simple/.env`）。可通过 `WEGO_API_URL` / `WEGO_GRPC_ADDRESS` 覆盖地址。观测验收需要 Jaeger OTLP `localhost:4317` 和查询 API `localhost:16686`。

完整门禁与本机验收统一入口为 `sdks/wego/scripts/acceptance.py`，完成后生成 `docs/design/acceptance-report.json` 和 Markdown 摘要。必需场景未执行或任何门禁失败时不会生成完成报告。

从仓库根目录运行（本机脚本自动选择 SDK 工作区）：

```sh
sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/retries
sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/migration-guides/temporal
sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/grpc-streams
sdks/wego/scripts/local-test.py go test -tags=e2e ./sdks/wego/tests/e2e/... -timeout 30m
sdks/wego/scripts/local-test.py go test -tags=e2e,wego_embedded ./sdks/wego/tests/e2e/... -run '^TestEmbedded$' -timeout 10m
```

每次生成独立 namespace，确认真实注册、调度与结果；删除测试定义和触发器，保留可追溯运行历史。JSON 验收报告位于 `.test-results/`，不包含凭证。故障用例为 `TestStreamFaults`，Worker 进程重启用例为 `TestDurableRestart`，并发 batch 关闭预算用例为 `TestBatchShutdownBudget`。最终本机结果见 [验收报告](docs/design/acceptance-report.md)。

Embedded 依赖仅由 `wego_embedded` tag 启用。验收在独立进程、独立 PostgreSQL 库和空闲端口运行；默认数据库管理连接使用本机 root 配置，可用 `WEGO_EMBEDDED_ADMIN_DATABASE_URL` 覆盖。独立示例从 `examples/embedded` 目录运行 `GOWORK=off go run -tags=wego_embedded .`；使用 `WEGO_EMBEDDED_DATABASE_URL` 指向专用 UTC 测试库。独立 module 固定官方发行依赖，避免工作区根模块的 `(devel)` 版本使上游启动器解析失败。

## 构建与质量门禁

```sh
# Go 依赖和工作区完全放在 SDK 内，根 go.mod/go.sum 保持原样。
export GOWORK="$PWD/sdks/wego/go.work"
sdks/wego/scripts/install-tools.sh
sdks/wego/scripts/generate.sh
go run ./sdks/wego/scripts/manifest -check
go test ./sdks/wego/...
go vet ./sdks/wego/...
go test -race ./sdks/wego/...
go test ./sdks/go/... ./pkg/client/... ./pkg/worker/...
go test ./sdks/wego/internal/wire -run '^$' -fuzz FuzzDecodeFrame -fuzztime=10s
```

生成工具固定为 protoc 29.6、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.5.1；生成范围仅为 wego 协议和示例业务桩。CI 的 [compose 配置](tests/compose.yml) 使用 PostgreSQL MQ，并显式设置 `SERVER_SECURITY_CHECK_ENABLED=false`。

## 运行约定

`runtime.WithResultPollInterval(2 * time.Second)` 调整结果等待的辅助取消状态查询间隔，默认 1 秒；结果主订阅与调用方 context 取消不受该间隔影响。`runtime.StreamConfig.CancelTimeout` 设置流结束时 CANCEL 控制的独立预算，默认 5 秒且不超过实例清理预算。握手只保留一个在途 PING，重发以 250ms～1s 退避，READY 后回收 PING 等待再 OPEN。

- RPC 业务载荷为 protobuf 二进制，版本 3 JSON envelope 使用 base64。`runtime.WithInputProjection` 显式映射 CEL 字段到 `input.routing`；客户端和 Worker 使用相同配置。
- 使用 `task.Client(ctx)` 的借用连接保留父子身份与 durable 账本；稳定子调用用 `task.WithChildKey`。借用句柄不能关闭共享资源。
- 控制 Worker 独立 4 slots；会话固定 owner，窗口默认 64 条 / 4 MiB，业务消息上限 1 MiB。END 只半关闭输入，全部输出及 OK 终态确认后返回 EOF。
- 流不参与 durable 重放，也不自动重新分配 owner。错误通过 gRPC status/details 或 wego 错误返回。
- Server 使用 `GracefulStop()` 排空、`Stop()` 取消；Conn / 原生 Worker 保留 `Shutdown(ctx)`，默认关闭预算 30 秒。业务 handler 应响应 context。
- Trace provider 属于实例，默认 Hatchet 出口，可配置附加 exporter；应用传入的 exporter 由应用关闭。Metrics 默认关闭。
- 原生 JSON 定义保留 batch、Webhook、DAG 和官方任务输出订阅的语义。


当前通信协议为 **3**。metadata 的值按原始字节传输，支持 `-bin` 和多值；客户端和 Worker 必须同时升级。终态使用 `has_response` 区分合法的零字节 protobuf 响应与 handler 没有发送响应。公开 `client`、`features` 和 `telemetry` 不提供内部引擎或资源工厂。

本轮二次评审修复及回归入口见 [rereview-fixes.md](docs/design/rereview-fixes.md)；[review-fixes.md](docs/design/review-fixes.md) 保留首轮历史记录。完整验收使用 `scripts/acceptance.py`，每个官方构造片段必须具备本轮实际通过的断言证据。

构建也可以进入 `sdks/wego` 后执行 `go test ./...`。`go.work` 只用于本仓库开发；SDK 依赖在自己的 `go.mod/go.sum` 中声明。CI 模板保存在 [tests/ci.yml](tests/ci.yml)，本次没有新增仓库 `.github` 工作流。

发行依赖固定为官方 Hatchet **v0.109.0**，它提供可关闭的底层客户端和带 context 的 durable memo 完成接口。本机环境已升级为官方 **v0.110.5 / PostgreSQL MQ**；本轮验收结果见 [简洁性处理报告](docs/conciseness-fixes.md)，历史 v0.107.0 的结果按原版本保留。`python3 scripts/check-released.py` 在 `GOWORK=off` 下验证实际发行依赖，拒绝本地 `replace`；运行该检查不改写上游源码。

后端复用未经修改的官方执行器。它的公开注入接口接受 `pkg/client.Client`，因此 wego 私有保留该低层客户端；这并不要求使用旧版工作流定义 API。注册、提交、结果等待和 durable 桥接位于 `internal/backend`，不修改上游源码。官方客户端没有公开连接注入入口，wego 当前拥有执行连接和预算可控的协议连接两条通道，并统一关闭。

结果包含对象存储载荷时，给解码设置独立预算：

```go
result, err := ref.Result(ctx)
if err != nil { return err }
return result.IntoContext(ctx, &reply)
```

`Into` 使用实例配置的关闭预算；已取得的纯内存结果可以在 Conn 关闭后读取。`TaskOutput("task")` 区分存在的 JSON null 和缺失键，字符串 `"7"` 保持字符串类型。

深度评审 Q-001～Q-011 的处理决定、真实前后基准与验证证据见 [完成报告](docs/deep-quality-fixes.md)。历史验收不会改写为新版本结果。
