package backend

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// executionTraceContext 统一普通任务和可靠流的执行父链路，不修改全局 propagator。
// 官方调度字段优先，其次保留已有上下文；兼容读取 wego 前缀字段以排空已提交运行。
func executionTraceContext(ctx context.Context, metadata map[string]string) context.Context {
	carrier := propagation.MapCarrier{
		"traceparent": metadata["traceparent"],
		"tracestate":  metadata["tracestate"],
	}
	// 空上下文用于验证字段本身，避免把已有 span 误认为官方 metadata 解析成功。
	parent := propagation.TraceContext{}.Extract(context.Background(), carrier)
	if span := trace.SpanContextFromContext(parent); span.IsValid() {
		return trace.ContextWithRemoteSpanContext(ctx, span)
	}
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	carrier["traceparent"], carrier["tracestate"] = metadata["wego.traceparent"], metadata["wego.tracestate"]
	return propagation.TraceContext{}.Extract(ctx, carrier)
}
