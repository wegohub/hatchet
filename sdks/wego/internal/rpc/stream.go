package rpc

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/session"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// StreamHandler 将 gRPC 流描述绑定为会话 handler，业务使用标准 ServerStream。
func StreamHandler(e *engine.Engine, method binding.Method) session.Handler {
	return func(ctx context.Context, endpoint *session.Endpoint) (err error) {
		ctx = e.Context(ctx)
		// ctx, finish 开始属于当前实例的 span，结束时按实际执行错误标记状态。
		ctx, finish := e.StartSpan(ctx, method.FullName, trace.SpanKindServer)
		defer func() {
			finish(err)
		}()

		// stream 委托当前会话 Endpoint 的标准 gRPC ServerStream，业务继续使用生成桩的 Send 和 Recv。
		stream := &serverStream{
			endpoint: endpoint,
			ctx:      ctx,
			engine:   e,
			method:   method,
		}
		// handler 适配标准流 handler，序号、ACK 与半关闭由会话层负责。
		handler := grpc.StreamHandler(func(service any, stream grpc.ServerStream) error {
			return method.Stream.Handler(service, stream)
		})
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
			// current 当前层的拦截器，反向构造调用链以保持配置中外层到内层的执行顺序。
			current := e.Config.Middleware[i].StreamServer
			if current != nil {
				// next 构造当前层之前的内层调用闭包，捕获此值避免所有拦截器递归引用同一最终变量。
				next := handler
				handler = func(service any, stream grpc.ServerStream) error {
					return current(
						service,
						stream,
						&grpc.StreamServerInfo{
							FullMethod:     method.FullName,
							IsClientStream: method.Stream.ClientStreams,
							IsServerStream: method.Stream.ServerStreams,
						},
						next,
					)
				}
			}
		}
		return handler(method.Service, stream)
	}
}

// serverStream 将会话端点适配成标准 gRPC ServerStream。
type serverStream struct {
	// endpoint Worker 侧流会话端点，承担有界缓冲和顺序交付。
	endpoint *session.Endpoint
	// ctx 当前操作上下文，承载取消、截止时间与 metadata。
	ctx context.Context
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭。
	engine *engine.Engine
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称。
	method binding.Method
}

// Context 返回当前执行上下文，包含取消与截止时间。
func (s *serverStream) Context() context.Context {
	return s.ctx
}

// SetHeader 合并响应头缓存，不立即发送。
func (s *serverStream) SetHeader(md metadata.MD) error {
	return s.endpoint.SetHeader(md)
}

// SendHeader 合并并发布响应头；重复发送由当前传输的状态规则处理。
func (s *serverStream) SendHeader(md metadata.MD) error {
	return s.endpoint.SendHeader(md)
}

// SetTrailer 合并响应尾部 metadata，随最终状态返回。
func (s *serverStream) SetTrailer(md metadata.MD) {
	s.endpoint.SetTrailer(md)
}

// RecvMsg 按序接收并解码一条 protobuf 消息；完整终态确认后才返回 EOF。
// 例如输出到达顺序为 2、1、3，业务读取顺序仍为 1、2、3；最后状态非 OK 时返回错误。
func (s *serverStream) RecvMsg(message any) error {
	// p, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
	p, ok := message.(proto.Message)
	// 检查 !ok；不满足协议或配置约束时返回 Internal（wego: decoder expected protobuf）。
	if !ok {
		return status.Error(codes.Internal, "wego: decoder expected protobuf")
	}

	// payload, err 接收 s.endpoint.Receive 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	payload, err := s.endpoint.Receive(s.ctx)
	if err != nil {
		return err
	}

	return wire.Decode(
		s.ctx,
		s.method.FullName,
		wire.Envelope{Version: wire.Version, Payload: payload},
		p,
		s.engine.Config.Middleware,
		s.engine.Config.Stream.MaxMessageBytes,
	)
}

// SendMsg 编码并发送一条 protobuf 消息；窗口或字节预算不足时等待累计 ACK。
func (s *serverStream) SendMsg(message any) error {
	// p, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
	p, ok := message.(proto.Message)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
	if !ok {
		return fmt.Errorf("wego: encoder expected protobuf")
	}

	// envelope, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	envelope, err := wire.Encode(s.ctx, s.method.FullName, p, nil, s.engine.Config.Middleware, s.engine.Config.Stream.MaxMessageBytes)
	if err != nil {
		return err
	}
	if !s.method.Stream.ServerStreams {
		return s.endpoint.SetResponse(envelope.Payload)
	}

	return s.endpoint.Send(s.ctx, envelope.Payload)
}

// NewStream 将流的生命周期纳入实例排空，清理完成后才结束 span 和在途调用。
// 流会话有外部交互状态，不允许在 durable handler 中重放。
func NewStream(ctx context.Context, e *engine.Engine, description *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// 检查 ok && state.Execution != nil && state.Execution.Info().Durable；不满足协议或配置约束时返回 FailedPrecondition（wego: streams cannot participate in durable replay）。
	if state, ok := callctx.Get(ctx); ok && state.Execution != nil && state.Execution.Info().Durable {
		return nil, status.Error(codes.FailedPrecondition, "wego: streams cannot participate in durable replay")
	}

	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空。
	ctx, done, err := e.BeginIO(ctx, "rpc.stream")
	if err != nil {
		return nil, err
	}

	// ctx, finish 开始属于当前实例的 span，结束时按实际执行错误标记状态。
	ctx, finish := e.StartSpan(ctx, method, trace.SpanKindClient)
	// completed 只执行一次的完成或释放保护，避免重复关闭通知通道。
	var completed sync.Once
	// closed 在流订阅与会话清理完成后释放 Engine.Begin 登记，不能在 END 确认时提前释放。
	closed := func(err error) {
		completed.Do(func() {
			finish(err)
			done()
		})
	}
	// streamer 建立客户端流适配链，拦截器传入的参数必须传至真实调用。
	streamer := grpc.Streamer(func(ctx context.Context, desc *grpc.StreamDesc, _ *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return session.NewClientManaged(ctx, e.Backend, e.Config, desc, method, closed, opts...)
	})
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
		// current 当前层的拦截器，反向构造调用链以保持配置中外层到内层的执行顺序。
		current := e.Config.Middleware[i].StreamClient
		if current != nil {
			// next 构造当前层之前的内层调用闭包，捕获此值避免所有拦截器递归引用同一最终变量。
			next := streamer
			streamer = func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return current(ctx, desc, conn, method, next, opts...)
			}
		}
	}
	// stream, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	stream, err := streamer(ctx, description, nil, method, opts...)
	if err != nil {
		closed(err)
		return nil, err
	}

	return stream, nil
}
