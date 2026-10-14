# 载荷组件

`middleware/payload` 是 wego SDK 的公开载荷中间件组件包，实现父包的 `middleware.Payload` 契约。父包只提供接口和注册入口，不导入具体组件。通过 `middleware.WithPayload` 注册 `Gzip`、`Zstd`、`AESGCM` 或 S3 `Objects`；`runtime.WithEncodingCodecs` 指定发送顺序。接收端按消息中的 CodecID 逆序解码，注册顺序不影响历史消息读取。

```go
gzipCodec, err := payload.NewGzip(payload.WithThreshold(256 << 10))
if err != nil { return err }
encrypted, err := payload.NewAESGCM(key) // key 为 32 字节
if err != nil { return err }

opts := []runtime.Option{
    runtime.WithMiddleware(
        middleware.WithPayload(gzipCodec),
        middleware.WithPayload(encrypted),
    ),
    runtime.WithEncodingCodecs(payload.GzipCodecID, payload.AESGCMCodecID),
}
```

gzip、zstd 和 S3 默认从 256 KiB 开始执行；`WithThreshold(0)` 表示始终执行。AES-GCM 始终加密，密钥由应用提供。CodecID 是持久格式身份：`gzip.v1`、`zstd.v1`、`aes256-gcm.v2`、`s3-object.sha256.v1`。移动包位置不改变这些值或已有消息字节。

`NewS3` 当前使用显式凭证、自定义 S3 端点、`us-east-1` 区域和路径寻址；构造时检查桶，缺失时创建。应用负责保留存储对象、旧解码组件和密钥，直到相关任务及通知历史不再需要恢复。应用退出时调用 `Objects.Close()` 释放组件自己的 HTTP 空闲连接。

旧的 `examples/codec/payload` 路径只保留转发；新代码直接导入 `github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload`。
