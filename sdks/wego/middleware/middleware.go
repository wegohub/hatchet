package middleware

import (
	"context"

	"google.golang.org/grpc"
)

// Payload 按调用路径处理业务消息或完整协议帧；默认按注册顺序编码，解码按消息携带的 CodecID 逆序执行
type Payload interface {
	// CodecID 标识持久格式，不能随加密 nonce 或当前密钥变化。
	CodecID() string
	// DecodeLimit 在下载、解压和分配过程中限制还原字节；例如最多 1 MiB。
	DecodeLimit(context.Context, []byte, int) ([]byte, error)
	// Encode 按配置顺序变换业务字节，例如压缩、加密或上传对象；I/O 必须尊重 context
	Encode(context.Context, []byte) ([]byte, error)
	// Decode 反向还原业务字节，例如先解密再解压；失败不能继续 protobuf 解码
	Decode(context.Context, []byte) ([]byte, error)
}

// ThresholdPayload 为压缩或卸载组件声明默认发送阈值；解码不受阈值影响。
// 阈值按该步骤收到的字节计算，例如压缩后不足 256 KiB 的消息不再上传。
type ThresholdPayload interface {
	// Payload 保持统一有界编解码契约，阈值只控制发送步骤。
	Payload
	// EncodingThreshold 返回最小触发字节数；0 表示始终执行。
	EncodingThreshold() int
}

// FramedPayload 与 Payload 使用同一个有界契约，帧和普通任务没有额外解码分支。
type FramedPayload = Payload

// Option 组合 Worker 拦截器与载荷变换；持久流的业务和控制字段一起按完整帧变换
type Option struct {
	// UnaryServer Worker unary 服务端拦截器，不作用于原生网络入口
	UnaryServer grpc.UnaryServerInterceptor
	// UnaryClient Worker unary 客户端拦截器
	UnaryClient grpc.UnaryClientInterceptor
	// StreamServer Worker 流服务端拦截器
	StreamServer grpc.StreamServerInterceptor
	// StreamClient Worker 流客户端拦截器
	StreamClient grpc.StreamClientInterceptor
	// Payload 处理 ProtoJSON 业务输入及完整流帧；变换后的字节由传输层包装
	Payload Payload
}

// WithUnaryServer 配置 Worker unary 服务端拦截器；网络入口通过 WithGRPC 配置原生选项
func WithUnaryServer(interceptor grpc.UnaryServerInterceptor) Option {
	return Option{UnaryServer: interceptor}
}

// WithUnaryClient 配置 Worker unary 客户端拦截器
func WithUnaryClient(interceptor grpc.UnaryClientInterceptor) Option {
	return Option{UnaryClient: interceptor}
}

// WithStreamServer 配置 Worker 流服务端拦截器
func WithStreamServer(interceptor grpc.StreamServerInterceptor) Option {
	return Option{StreamServer: interceptor}
}

// WithStreamClient 配置 Worker 流客户端拦截器
func WithStreamClient(interceptor grpc.StreamClientInterceptor) Option {
	return Option{StreamClient: interceptor}
}

// WithPayload 注册载荷组件，并加入默认发送链；流控制帧同样经过完整变换。
// 接收端按消息 ID 查找组件，注册顺序不影响解码；发送链可由 Runtime 显式指定。
func WithPayload(payload Payload) Option {
	return Option{Payload: payload}
}
