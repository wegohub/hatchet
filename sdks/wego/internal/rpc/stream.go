package rpc

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
)

// NewStream 将流的生命周期纳入实例排空，清理完成后才结束 span 和在途调用
// 流会话有外部交互状态，不允许在 durable handler 中重放
func NewStream(ctx context.Context, e *engine.Engine, description *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// 检查 ok && state.Execution != nil && state.Execution.Info().Durable；不满足协议或配置约束时返回 FailedPrecondition（wego: streams cannot participate in durable replay）
	if state, ok := callctx.Get(ctx); ok && state.Execution != nil && state.Execution.Info().Durable {
		return nil, status.Error(codes.FailedPrecondition, "wego: streams cannot participate in durable replay")
	}

	initial, parseErr := ParseCallOptions(opts, false)
	if parseErr != nil {
		initial.Finish(&parseErr)
		return nil, parseErr
	}
	// created 仅记录真实传输，外层拦截器创建后再失败时必须主动清理。
	var created *taskClientStream
	streamer := grpc.Streamer(func(ctx context.Context, desc *grpc.StreamDesc, _ *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, done, err := e.BeginIO(ctx, "rpc.stream")
		if err != nil {
			return nil, err
		}
		ctx, finish := e.StartSpan(ctx, method, trace.SpanKindClient)
		// completed 防止启动失败和正常关闭重复释放登记。
		var completed sync.Once
		closed := func(err error) { completed.Do(func() { finish(err); done() }) }
		stream, err := newTaskClient(ctx, e, desc, method, closed, opts...)
		if err != nil {
			closed(err)
			return nil, err
		}
		created = stream.(*taskClientStream)
		return stream, nil
	})
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
		// current 当前层的拦截器，反向构造调用链以保持配置中外层到内层的执行顺序
		current := e.Config.Middleware[i].StreamClient
		if current != nil {
			// next 构造当前层之前的内层调用闭包，捕获此值避免所有拦截器递归引用同一最终变量
			next := streamer
			streamer = func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return current(ctx, desc, conn, method, next, opts...)
			}
		}
	}
	// stream, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	stream, err := streamer(ctx, description, nil, method, opts...)
	if err != nil {
		if created != nil {
			created.cancel()
			created.finish(err)
		} else {
			initial.Finish(&err)
		}
		return nil, err
	}

	return stream, nil
}
