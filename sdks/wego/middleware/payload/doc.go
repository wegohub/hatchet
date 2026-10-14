// Package payload 提供 gzip、zstd、AES-GCM 和 S3 载荷中间件组件。
// 组件实现父包 middleware 的契约，由 middleware.WithPayload 注册；
// 例如发送按 gzip→AES-GCM 编码，接收按消息记录的 CodecID 逆序解码。
// 本包只负责字节变换和组件资源，协议封包及链路选择由 SDK 统一处理。
package payload
