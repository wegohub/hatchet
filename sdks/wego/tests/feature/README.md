# 手动 feature 测试

Worker 和 Conn 在各自启动文件中直接配置：显式 `WithEncodingCodecs` 选择 zstd → AES-GCM → S3 上传，另注册 gzip 供解码使用；接收时根据消息 CodecID 逆序下载、解密、解压。S3 使用 AWS SDK、路径寻址，连接本机 MinIO。

两端加载同一份私有 `.env`，需要：

```dotenv
WEGO_CODEC_AES_KEY=<64 位十六进制密钥>
WEGO_S3_ENDPOINT=http://127.0.0.1:9000
WEGO_S3_BUCKET=wego
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<MinIO 用户>
AWS_SECRET_ACCESS_KEY=<MinIO 密码>
```

现有本机配置已补齐；这些凭证不要提交到仓库。Client 自行加载 `../.env`，Worker 仍通过原有 `godotenv -f ../.env go run main.go` 启动。

所有实例使用 `feature/` 对象前缀，允许独立进程和重启后的实例读取历史。不要在运行历史仍需恢复时删除这些对象。关闭只释放 HTTP 空闲连接。

Console 中业务 `value` 将显示为对象引用的 base64，`_wego.codec_ids` 记录三个变换步骤；routing 仍为明文。修改配置后重启 Worker 和 Client。

压缩（gzip/zstd）与 S3 默认阈值为 256 KiB，按每一步收到的实际字节数判断，达到阈值才执行。例如原始 300 KiB 经 zstd 压缩后变为 20 KiB，后续 S3 不上传；AES 仍执行。`payload.NewZstd(payload.WithThreshold(128<<10))` 可改为 128 KiB，`payload.NewS3(ctx, config, prefix, payload.WithThreshold(0))` 强制上传。消息只记录实际执行的 CodecID，接收端阈值不影响解码。直接调用组件 Encode 不应用发送链阈值。

`Test_RunMany` 与 `Test_Concurrency` 都将十项请求分成三个组。当前 Worker 以 group 配置 STATUS 幂等，批量直接提交应返回三个 RunRef 和七次 `BulkIdempotencyCollisionError` 冲突；事件发布则不向发布者报告这些冲突。批量用例使用独立分组前缀，避免与其他测试互相去重，并等待三个实际结果。直接 RunMany 不使用事件 Filter Scope。

先启动 Worker，再单独执行：

```bash
go test ./tests/feature/client -run '^Test_RunMany$' -count=1 -v
```

`WaitHello` 作为 durable 父任务，基于 `task.Now` 保存的时间生成固定随机种子，每次逻辑运行创建 3–10 个 `SayHello` 子任务。子任务使用稳定 child key 和独立 routing.group，重放时复用身份；父任务通过 `pb.NewGreeterClient(conn)` 并发调用子任务，使用 `client.WithRunOptions` 传递配置，等待全部调用后按输入顺序收集结果，返回 `count`、`child_run_ids` 和 `child_results`。代码中的 `RunMany` 为注释替代示例。

```bash
go test ./tests/feature/client -run '^Test_ChildSpawning$' -count=1 -v
```

该用例核对每项业务输出、唯一子 RunID 和 ParentRunID。需先启动当前 feature Worker；子任务约执行 10 秒，父任务和测试的预算均为 2 分钟。客户端和 Worker 都应使用包含 `child_results` 的当前生成桩。
