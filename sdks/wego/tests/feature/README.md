# 手动 feature 测试

Worker 和 Conn 在各自启动文件中直接配置：显式 `WithEncodingCodecs` 选择 zstd → AES-GCM → S3 上传，另注册 gzip 供解码使用；接收时根据消息 CodecID 逆序下载、解密、解压。S3 使用 AWS SDK、路径寻址，连接本机 MinIO。

两端加载同一份私有 `.env`，需要：

```dotenv
WEGO_CODEC_AES_KEY=<64 位十六进制密钥>
WEGO_S3_ENDPOINT=http://127.0.0.1:9000
WEGO_S3_BUCKET=wego-codec
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<MinIO 用户>
AWS_SECRET_ACCESS_KEY=<MinIO 密码>
```

现有本机配置已补齐；这些凭证不要提交到仓库。Client 自行加载 `../.env`，Worker 仍通过原有 `godotenv -f ../.env go run main.go` 启动。

所有实例使用 `feature/` 对象前缀，允许独立进程和重启后的实例读取历史。不要在运行历史仍需恢复时删除这些对象。关闭只释放 HTTP 空闲连接。

Console 中业务 `value` 将显示为对象引用的 base64，`_wego.codec_ids` 记录三个变换步骤；routing 仍为明文。修改配置后重启 Worker 和 Client。

压缩（gzip/zstd）与 S3 默认阈值为 256 KiB，按每一步收到的实际字节数判断，达到阈值才执行。例如原始 300 KiB 经 zstd 压缩后变为 20 KiB，后续 S3 不上传；AES 仍执行。`payload.NewZstd(payload.WithThreshold(128<<10))` 可改为 128 KiB，`payload.NewS3(ctx, config, prefix, payload.WithThreshold(0))` 强制上传。消息只记录实际执行的 CodecID，接收端阈值不影响解码。直接调用组件 Encode 不应用发送链阈值。
