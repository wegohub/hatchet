package middleware

import (
	"context"

	"google.golang.org/grpc"
)

// Payload 只变换业务消息字节；编码按注册顺序执行，解码按相反顺序还原。
type Payload interface {
	// Encode 按配置顺序变换业务字节，例如压缩、加密或上传对象；I/O 必须尊重 context。
	Encode(context.Context, string, []byte) ([]byte, error)
	// Decode 反向还原业务字节，例如先解密再解压；失败不能继续 protobuf 解码。
	Decode(context.Context, string, []byte) ([]byte, error)
}

// Option 组合 Worker 拦截器与业务载荷变换，控制帧保持独立协议。
type Option struct {
	// UnaryServer Worker unary 服务端拦截器，不作用于原生网络入口。
	UnaryServer grpc.UnaryServerInterceptor
	// UnaryClient Worker unary 客户端拦截器。
	UnaryClient grpc.UnaryClientInterceptor
	// StreamServer Worker 流服务端拦截器。
	StreamServer grpc.StreamServerInterceptor
	// StreamClient Worker 流客户端拦截器。
	StreamClient grpc.StreamClientInterceptor
	// Payload 业务载荷；wire 中为 protobuf 字节，JSON 编码时自动转为 base64。
	Payload Payload
}

// WithUnaryServer 配置 Worker unary 服务端拦截器；网络入口通过 WithGRPC 配置原生选项。
func WithUnaryServer(interceptor grpc.UnaryServerInterceptor) Option {
	return Option{UnaryServer: interceptor}
}

// WithUnaryClient 配置 Worker unary 客户端拦截器。
func WithUnaryClient(interceptor grpc.UnaryClientInterceptor) Option {
	return Option{UnaryClient: interceptor}
}

// WithStreamServer 配置 Worker 流服务端拦截器。
func WithStreamServer(interceptor grpc.StreamServerInterceptor) Option {
	return Option{StreamServer: interceptor}
}

// WithStreamClient 配置 Worker 流客户端拦截器。
func WithStreamClient(interceptor grpc.StreamClientInterceptor) Option {
	return Option{StreamClient: interceptor}
}

// WithPayload 追加业务载荷变换，编码按顺序执行、解码按反序还原，不变换流控制帧。
func WithPayload(payload Payload) Option {
	return Option{Payload: payload}
}
