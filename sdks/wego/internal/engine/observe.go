package engine

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// StartSpan 用实例 provider 开始一次追踪，不修改 OTel 全局 provider。
func (e *Engine) StartSpan(ctx context.Context, method string, kind trace.SpanKind) (context.Context, func(error)) {
	// attrs 标识 RPC 方法、实例 namespace 和实际协议版本，供 trace 按执行来源关联。
	attrs := []attribute.KeyValue{
		attribute.String("instrumentor", "hatchet"),
		attribute.String("rpc.system", "grpc"),
		attribute.String("rpc.method", method),
		attribute.String("wego.namespace", e.Config.Namespace),
		attribute.Int("wego.protocol.version", wire.Version),
	}
	// state, ok 取得明确类型的字段或上下文能力，存在标记为 false 时不得使用。
	if state, ok := callctx.Get(ctx); ok && state.Execution != nil {
		// info 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
		info := state.Execution.Info()
		attrs = append(
			attrs,
			attribute.String("hatchet.workflow_run_id", info.RunID),
			attribute.String("hatchet.step_run_id", info.TaskRunID),
			attribute.String("hatchet.worker_id", info.WorkerID),
			attribute.Int("hatchet.retry_count", info.RetryCount),
		)
	}
	// ctx, span 使用所属实例的 tracer，保持全局 OTel provider 不变。
	ctx, span := e.Tracer().Start(ctx, method, trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
	// start 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
	start := time.Now()
	return ctx, func(err error) {
		// side 按 span 类型区分调用方、服务端与原生任务，指标使用同一入口分类。
		side := "client"
		if kind == trace.SpanKindServer {
			side = "server"
		} else if kind == trace.SpanKindInternal {
			side = "task"
		}
		// code 构造或读取标准 gRPC 状态，保留 code 与业务 details。
		code := status.Code(err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			code = status.FromContextError(err).Code()
		}
		e.telemetry.Observe(method, side, code, time.Since(start))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
