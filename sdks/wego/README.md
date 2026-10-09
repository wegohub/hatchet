# wego SDK 0.2.9

wego 把标准 Go gRPC handler 和 protobuf 客户端桩接入 Hatchet standalone 任务。`Conn` 实现 `grpc.ClientConnInterface`，`Server` 实现 `grpc.ServiceRegistrar`；公开 API、回调值和错误链不暴露 Hatchet 类型。

本版采用协议 **4**，只依赖未经修改的官方 Hatchet **v0.110.5**。0.2.0 的 18 项质量门禁、28 个源文件/74 个构造片段及完整验收记录见 [历史验收报告](docs/design/acceptance-report.md)。0.2.1 增加真实 MinIO codec 示例，使用 AWS S3 SDK；部署与运行见 [codec 示例](examples/codec/README.md)，0.2.1 的验证见 [专项报告](docs/design/minio-codec-report.md)。

0.2.2 增加租户 Durable Streams 开启脚本，运行后隐藏输入 token；数据库配置及用法见 [本机部署说明](deployment/compose/README.md)。

Worker 注册自动追加 `worker_name` 标签，其值始终为最终展示名称；`runtime.WithLabels` 的其他标签保留，`runtime.WithInstanceName` 同时决定名称与该标签。

## 注册与调用

```go
cfg := []runtime.Option{
    runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
    runtime.WithAddress("localhost:7077"),
    runtime.WithServerURL("http://localhost:8080"),
    runtime.WithTLSConfig(nil),
    runtime.WithNamespace("demo"),
    runtime.WithRoutingDefaults(pb.Greeter_SayHello_FullMethodName,
        map[string]any{"group":"default", "cost":1}),
}
srv := wego.NewServer(server.WithRuntime(cfg...), server.WithGRPC(":9002"))
pb.RegisterGreeterServer(srv, &greeter{})
go func() {
    if err := srv.Serve(); err != nil { /* 应用上报错误 */ }
}()
defer srv.Stop()

conn, err := client.New(client.WithRuntime(cfg...))
// 处理 err；ctx 应有符合业务需要的 deadline。
defer conn.Close()
rpc := pb.NewGreeterClient(conn)
ctx = client.WithRouting(ctx, map[string]any{"group":"group-a"})
out, err := rpc.SayHello(ctx, request)
```

网络入口直接执行同一 handler；`runtime.WithDisableWorker()` 关闭 Server 的任务入口，纯网络模式无需 token。`server.WithGRPC` 透传原生 gRPC 选项；`WithWorker` 仅配置任务策略。

`server.WithWorker(worker.WithDisableMethod(methods...))` 可一次禁用多个完整 RPC 方法名，例如 `worker.WithDisableMethod(pb.Greeter_SayHello_FullMethodName, pb.Greeter_UploadHellos_FullMethodName)`。只禁止本实例注册及消费对应任务，其他 RPC 正常调度，网络入口仍可调用。该配置不删除已有 workflow；全部方法均不需要调度时使用 `runtime.WithDisableWorker()`。

任务 Worker 的实例通知订阅也使用 Durable Streams，因此启用 Worker 前，部署必须为租户启用该能力；仅注册 unary 也遵守此启动条件。纯网络入口不需要该能力。

namespace 默认空。Worker 默认名称为 `<namespace>-<hostname>-<随机 worker_key 前12位>`，`WithInstanceName` 覆盖展示名称。worker_key 按 SDK 实例生成，重连保持、重启变化，不允许业务指定。RPC workflow 为 `<namespace>-<完整 protobuf 服务名>-<方法名>`，副本共享定义及版本；停止 Worker 不自动删除或暂停定义。

完整注册、durable、三种流、恢复和通知用法见 [worker.md](docs/design/worker.md)；模块边界及质量门禁见 [module.md](docs/design/module.md)，设计约束见 [v0.2.0.md](docs/design/v0.2.0.md)。[acceptance.json](examples/acceptance.json) 映射 28 个官方源文件中的全部 74 个 standalone 构造片段及辅助演示。

## 统一日志

`runtime.WithLogger` 设置 wego log 包共享的输出，最后一次应用的显式配置生效；不修改 `slog.Default()`。未配置时使用标准默认日志器。context 只传递属性、trace 和任务身份。

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
    Level: slog.LevelDebug, AddSource: true,
}))
srv := wego.NewServer(server.WithRuntime(
    runtime.WithLogger(logger),
    runtime.WithLogReport(true),
    runtime.WithLogReportTimeout(5*time.Second),
    // 此处补充 token、地址与其他任务配置。
))
pb.RegisterGreeterServer(srv, &greeter{})
ctx := log.With(context.Background(), slog.String("component", "processor"))
go func() {
    if err := srv.Serve(); err != nil {
        log.Error(ctx, "server stopped", "error", err)
    }
}()
// 应用在退出时调用 srv.Stop() 或 srv.GracefulStop()。
```

普通 `Info` 等只打印；R 系列先打印再同步上报任务日志。普通或网络 context 调用 R 系列返回 `model.ErrTaskContext`；任务未开启上报返回 `model.ErrLogReportDisabled`，两种情况仍按级别打印。上报默认预算 5 秒，与调用 deadline 取较早者，不后台排队或重试。

`log.With(ctx, attrs...)` 继承字段，同层同名键后写覆盖。固定字段、context、本次参数依次覆盖；具名 Group 整体覆盖，无名 Group 展开，执行与有效 trace 身份最终由 SDK 补充。`log.FromContext(ctx)` 提供标准 `slog.Logger` 的 With/WithGroup 和日志方法；`log.Handler(h)` 支持直接接入标准 slog。外部 Handler 的私有固定属性与格式转换不能反向读取，两个出口共享的字段使用 `log.With` 或 R 参数提供。

## 单任务流与恢复

- 每个 RPC 对应一个 StandaloneTask。client/bidi 的 Send 保存独立快照，CloseSend 一次提交请求数组，Worker 逐条 Recv 至 EOF；任务正常占用 slots。
- Worker bidi 使用整批输入、流式输出。交互式双向通信使用网络 gRPC 入口。
- server/bidi 默认 Reliable，使用持久输出、CLAIM 竞争及最终清单核验；部署必须启用 Durable Streams。SDK 不直接修改数据库或静默降级。
- `runtime.WithStreamMode(fullMethod, runtime.Realtime)` 显式使用 PutStream。该路径不能持久回放，官方订阅没有就绪 ACK，输出缺失或断流明确报错。
- `client.WithIdempotencyKey` 指定业务保存的逻辑调用键；`client.StreamCheckpoint` 只在成功交付输出后推进，`conn.ResumeStream` 从已有运行继续消费，不追加输入。
- `task.Checkpoint` 返回本次接管前的有限历史；有历史的 handler 必须读到 EOF 后恢复业务进度，再发送新输出。外部副作用及业务消费幂等由应用负责。

业务请求为 ProtoJSON，CEL 使用显式 `input.routing.*`。routing 不经过载荷变换；所有流帧，包括 CLAIM、HEADERS、DATA、结束、JOIN、事件和取消，统一经过有界 codec。不明确发布结果复用同一 producer、序号和已编码字节。

默认单条业务消息 1 MiB、输入总量 4 MiB、最多 4096 条输入、输出预取 64 条/4 MiB；按方法配置 `WithStreamOptions`。整帧、后端完整请求、对象下载、解压、扫描、恢复和 I/O 分别有预算，超限整批失败，不提交部分输入。

## 运行与质量门禁

本机默认 API `http://localhost:8080`、gRPC `localhost:7077`，明文、PostgreSQL MQ。设置 `HATCHET_CLIENT_TOKEN`，或由脚本读取 `WEGO_TOKEN_FILE`（JWT 或 `.env`，默认 `examples/go/simple/.env`）。`WEGO_API_URL` / `WEGO_GRPC_ADDRESS` 可覆盖地址；trace 验收使用 Jaeger OTLP `localhost:4317` 和查询 API `localhost:16686`。

从仓库根目录运行，显式关闭开发工作区以验证发行依赖：

```sh
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego run ./examples/retries
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego run ./examples/grpc-streams
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego run ./examples/codec
GOWORK=off go -C sdks/wego test ./...
GOWORK=off go -C sdks/wego vet ./...
GOWORK=off go -C sdks/wego test -race ./...
python3 sdks/wego/scripts/acceptance.py
```

完整运行器验证公开 API/动态类型隔离、导入/源码边界、中文注释、生成复现、race、两类协议 fuzz、官方回归和全部真实引擎场景；全部必需项通过才生成当前版本的完成报告。生成工具固定 protoc 29.6、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.5.1，仅生成 wego 协议与业务桩。

测试使用唯一 namespace，记录实际命令、身份、断言和清理，不记录凭证。业务定义及触发器清理；运行历史、持久 topic、引擎 Worker 记录和明确保留的协议探针定义由官方保留策略管理。CI 的 [compose](tests/compose.yml) 使用 PostgreSQL MQ，显式设置 `SERVER_SECURITY_CHECK_ENABLED=false`。

本机 [部署模板](deployment/compose/README.md) 包含 MinIO：S3 API 为 `127.0.0.1:9000`，控制台为 `127.0.0.1:9001`。真实 codec 验收需要 S3 凭证和 AES 密钥；本机启动脚本从私有部署文件读取，显式环境优先。持久任务/topic 引用的 S3 对象和历史密钥必须一起保留。

Embedded 仅由 `wego_embedded` tag 启用，使用独立进程、测试库和空闲端口。部署 fixture 在启动 Worker 前启用 Durable Streams entitlement；业务环境由部署初始化完成。独立示例在 `examples/embedded` 执行 `GOWORK=off go run -tags=wego_embedded .`，`WEGO_EMBEDDED_DATABASE_URL` 必须指向专用 UTC 数据库。真实验收还验证独立示例、迁移、调用、关闭与删除测试库。

## 生命周期与通知

Server 使用阻塞的 `Serve`，应用通过 `Stop` 或 `GracefulStop` 显式关闭；进程信号由应用自行处理。Conn 和原生 Worker 保留带调用方预算的关闭入口；默认独立清理预算 30 秒，Stop 能中断排空。排空期间保留事件订阅，确认输出、结果和日志后再关闭订阅、flush telemetry 和关闭连接。

RPC、同步 Run、`RunRef.Result(ctx)` 的 context 取消会请求取消共享运行；其他观察者也会看到取消。RunNoWait/RunMany 的 context 只控制提交，成功返回后的运行使用显式 Cancel。提交响应丢失时返回可诊断的提交错误及可恢复身份，不能把未知结果当成没有提交。

`conn.Workers().SendEvent` 定向通知实例，`BroadcastEvent` 通知同 tenant/namespace 当前已加入的 Worker；成功表示持久发布，不表示业务回调已处理。JOIN、有限重连及内部取消不占 slots；业务回调使用独立有界队列。

Trace provider 和 metrics registry 属于实例，不替换全局 OTel provider，metrics 默认关闭。协议指标只使用 method/mode/outcome 标签，运行身份通过日志/trace 关联。借用连接与应用提供的 exporter/存储由其所有者关闭。

升级前排空协议 3 的任务，协调客户端和 Worker 升级，并更新 workflow、Cron、Schedule、Event 引用；本版不转换历史运行或自动删除旧定义。Go SDK 的后续 Streams 更新通过私有 backend 适配，须重新核验 producer/seq/cursor/错误与连接生命周期契约。

覆盖率、复杂度和性能基线可在独立 SDK 模块运行：

```bash
python3 scripts/quality-baselines.py --collect --complexity --benchmarks
GOWORK=off go test -tags=e2e ./tests/e2e -run '^TestStreamPerformanceBaseline$' -timeout 12m
```

统计同时保存原始全量与排除生成/示例代码后的生产覆盖率。协议包最低 70%，其他包按已测基线防止下降；复杂度工具固定版本，现存高复杂度函数禁止增长。性能报告包含实际输出断言和延迟分位数，不设未经测量的 QPS 保证。帧 encode/decode 次数与耗时、输出预取条数及字节数使用实例指标，默认关闭。

`scripts/unit.py` 和 `scripts/unit.py -race` 运行 SDK 与质量门禁；`tests/feature/client` 是需要单独启动 Worker 的手动入口，明确排除于该单元范围，必需 RPC 场景由 `tests/e2e` 验证。直接 `go test ./...` 仍会执行手动入口，需要准备它的 Worker 与私有配置。
