# MinIO codec 示例

使用 **AWS SDK for Go v2 的 S3 客户端**连接 MinIO，配置 `BaseEndpoint` 和 `UsePathStyle=true`。示例实现见 [payload](payload/s3.go)，业务调用及断言见 [MinIOCodec](../scenarios/minio_codec.go)。不依赖 MinIO 专用 Go 客户端。

先按 [部署说明](../../deployment/compose/README.md) 启动 MinIO。本机 Hatchet 租户需启用 Durable Streams，连接地址与现有示例相同。从仓库根目录运行：

```sh
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego run ./examples/codec
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego test -race -tags=e2e ./tests/e2e -run '^TestMinIOCodec$' -count=1 -v -timeout 5m
```

也可独立配置环境运行；显式环境优先于本机私有 `.env`：

| 配置 | 作用 |
|---|---|
| WEGO_S3_ENDPOINT | 默认 http://127.0.0.1:9000 |
| WEGO_S3_BUCKET | 默认 wego-codec |
| AWS_REGION | 本例建桶路径使用 us-east-1 |
| AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY | S3 凭证，无公共默认值 |
| WEGO_CODEC_AES_KEY | 32 字节密钥的 64 位十六进制编码 |
| HATCHET_CLIENT_TOKEN | 现有 Hatchet 令牌 |
| WEGO_COMPOSE_ENV | 覆盖脚本读取的私有部署文件路径 |

业务配置直接组合 middleware：

```go
objects, err := payload.NewS3(ctx, payload.ConfigFromEnv(), "application-streams/")
// 检查 err；所有客户端、Worker 副本及恢复进程共用相同存储范围和密钥。
defer objects.Close()
encrypted, err := payload.NewAESGCM(key)
// 检查 err；key 为 32 字节独立配置。
gzipCodec, err := payload.NewGzip(payload.WithThreshold(256 << 10))
if err != nil {
    return err
}
codec := runtime.WithMiddleware(
    middleware.WithPayload(gzipCodec),
    middleware.WithPayload(encrypted),
    middleware.WithPayload(objects),
)
// 可显式指定发送链；Conn 和 Server 可有不同顺序，但各自必须具备接收所需的组件。
encoding := runtime.WithEncodingCodecs(payload.GzipCodecID, payload.AESGCMCodecID, payload.S3ObjectCodecID)
// 将 codec、encoding 放入实例 runtime；WithEncodingCodecs() 仅关闭发送变换。
```

编码顺序为 gzip → AES-256-GCM → S3 上传；解码依据消息保存的 CodecID 反向执行；注册顺序不同或额外注册组件不影响解码。wego 输入和响应、流的 CLAIM/HEADERS/DATA/结束、Worker JOIN/EVENT 经过同一 codec；原生调用按自身响应契约返回业务 JSON，routing 保持 JSON 可读。AES 使用显式密钥、随机 nonce 和 nil AAD，格式 ID 为 `aes256-gcm.v2`；外层 routing 和 metadata 不受加密认证绑定。随机 nonce 不作为格式身份；发布重试复用 SDK 已冻结的引用字节。

S3 引用只包含版本、目录内 key、大小及 SHA256，不携带地址或凭证。固定 bucket/prefix，拒绝跨目录和坏引用；实际下载、解压及认证解密均有字节预算，每次 S3 I/O 最长 10 秒，并服从外层更短 context。

示例验证 unary 分组并发、client/server/bidi、多消息及空流、慢消费、提前返回和业务错误；server stream 首次发送两条后重试，Worker 从历史恢复，独立 Conn 用序列化 checkpoint 继续消费。还验证二进制 metadata、完整 JOIN/EVENT、缺失对象与下载超限。单元测试覆盖签名、真实响应超过声明大小、篡改、错误密钥解密和取消。缺少 MinIO 或凭证时验收失败，不 Skip。

每次测试生成唯一 namespace 和对象 prefix，全部连接和 Worker 关闭，工作流定义与探针对象清理。**被持久任务/topic 引用的对象保留**，以便继续回放；对象、endpoint/bucket/prefix 配置、历史格式组件和解密密钥的有效期必须覆盖整个历史保留期。实际应用的 prefix 应跨 Worker 重启保持一致，不能按 Worker 实例随机生成。

测试证据保存至 `.test-results/minio-codec-*.json`，包括 RunID、WorkerID、对象统计与清理结果，不包含凭证。独立示例打印同样的已通过断言。


支持 `payload.Zstd`（`payload.ZstdCodecID`，`zstd.v1`）。可注册 gzip 与 zstd，再通过 `runtime.WithEncodingCodecs(payload.ZstdCodecID, payload.AESGCMCodecID, payload.S3ObjectCodecID)` 选择发送链，接收端按消息 ID 解码。所有组件 ID 已移除 `example.` 前缀；两端需协调切换，旧 ID 历史不自动兼容。

压缩（gzip/zstd）与 S3 默认阈值为 256 KiB，按每一步收到的实际字节数判断，达到阈值才执行。例如原始 300 KiB 经 zstd 压缩后变为 20 KiB，后续 S3 不上传；AES 仍执行。`payload.NewZstd(payload.WithThreshold(128<<10))` 可改为 128 KiB，`payload.NewS3(ctx, config, prefix, payload.WithThreshold(0))` 强制上传。消息只记录实际执行的 CodecID，接收端阈值不影响解码。直接调用组件 Encode 不应用发送链阈值。
