// Package extra 以经过 Hatchet 鉴权的 gRPC 接口提供内嵌 JetStream 消息、确认和 KV。
//
// 租户身份只来自 Bearer token；namespace 是租户内部的寻址分区，不是认证身份。
// 业务传入逻辑资源名称，服务负责映射所有嵌套发布和订阅地址。业务值是原始字节，
// 服务不隐式编解码。读取消息不代表确认，客户端须使用投递 token 显式 ACK。
//
// Server 只能启动一次。Shutdown 排空 RPC，Stop 可强制打断；关闭会取消并等待
// 实例的鉴权回源，随后关闭 NATS 连接和引擎。鉴权内存缓存不依赖 JetStream。
package extra
