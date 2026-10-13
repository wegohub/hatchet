package backend

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TestExecutionTracePriority 验证官方字段、现有上下文、兼容字段的优先级及无效父链路。
func TestExecutionTracePriority(t *testing.T) {
	engine := "00-11111111111111111111111111111111-1111111111111111-01"
	caller := "00-22222222222222222222222222222222-2222222222222222-01"
	current := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": caller})
	cases := []struct {
		// name 描述执行上下文来源，不依赖生产环境身份。
		name string
		// ctx 与 metadata 共同提供父 trace，want 为最终 trace ID。
		ctx context.Context
		// metadata 模拟官方 action 下发的运行 metadata。
		metadata map[string]string
		// want 为空时不应恢复任何有效 span。
		want string
	}{
		{"engine", current, map[string]string{"traceparent": engine, "tracestate": "vendor=state", "wego.traceparent": caller}, "11111111111111111111111111111111"},
		{"current", current, map[string]string{"traceparent": "invalid", "wego.traceparent": engine}, "22222222222222222222222222222222"},
		{"legacy", context.Background(), map[string]string{"wego.traceparent": caller}, "22222222222222222222222222222222"},
		{"missing", context.Background(), nil, ""},
		{"invalid", context.Background(), map[string]string{"traceparent": "invalid"}, ""},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			span := trace.SpanContextFromContext(executionTraceContext(item.ctx, item.metadata))
			if item.want == "" {
				if span.IsValid() {
					t.Fatal("invalid metadata created parent span")
				}
				return
			}
			if span.TraceID().String() != item.want || !span.IsRemote() {
				t.Fatalf("wrong execution parent: %v", span)
			}
			if item.name == "engine" && span.TraceState().String() != "vendor=state" {
				t.Fatal("engine tracestate lost")
			}
		})
	}
}

// TestTraceMetadataOfficialKeys 验证提交使用官方键，保留业务字段并复制调用者 map。
func TestTraceMetadataOfficialKeys(t *testing.T) {
	parent := "00-11111111111111111111111111111111-1111111111111111-01"
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parent, "tracestate": "vendor=state"})
	input := map[string]string{"source": "fixture", "traceparent": "invalid"}
	output := traceMetadata(ctx, input)
	if output["traceparent"] != parent || output["tracestate"] != "vendor=state" || output["source"] != "fixture" || output["wego.traceparent"] != "" || input["traceparent"] != "invalid" {
		t.Fatalf("incorrect metadata snapshot: %v", output)
	}
	// 当前 span 不含 tracestate 时不能保留运行选项中的旧状态。
	plain := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parent})
	clean := traceMetadata(plain, map[string]string{"tracestate": "other=stale"})
	if clean["tracestate"] != "" {
		t.Fatal("stale tracestate retained")
	}
	output = traceMetadata(context.Background(), input)
	if output["traceparent"] != "invalid" {
		t.Fatal("missing span altered caller metadata")
	}
}
