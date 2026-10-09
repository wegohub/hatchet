package middleware

import (
	"context"

	"google.golang.org/grpc"
)

// Payload 按调用路径处理业务消息或完整协议帧；编码按注册顺序，解码按相反顺序
type Payload interface {
	// Encode 按配置顺序变换业务字节，例如压缩、加密或上传对象；I/O 必须尊重 context
	Encode(context.Context, string, []byte) ([]byte, error)
	// Decode 反向还原业务字节，例如先解密再解压；失败不能继续 protobuf 解码
	Decode(context.Context, string, []byte) ([]byte, error)
}

// FramedPayload 为持久流提供稳定 codec 标识和有界还原能力
// 同一个实现可能并发处理 DATA、CLAIM、事件和恢复读取，必须支持并发调用
type FramedPayload interface {
	// Payload 提供相同的编码入口，控制帧与 DATA 使用同一变换链
	Payload
	// CodecID 返回稳定格式标识，不随当前写入密钥变化；恢复所需 key ID 保存在 codec 载荷中
	CodecID() string
	// DecodeLimit 在分配、解压或下载过程中检查上限，不能先无限读取再比较长度
	DecodeLimit(context.Context, string, []byte, int) ([]byte, error)
}

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

// WithPayload 追加变换，编码按顺序执行、解码按反序还原；流控制帧同样经过完整变换
func WithPayload(payload Payload) Option {
	return Option{Payload: payload}
}
