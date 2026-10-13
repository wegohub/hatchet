package wire

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TestIncomingKeepsExecutionTrace 验证请求协议头不能覆盖引擎已确定的父 span。
func TestIncomingKeepsExecutionTrace(t *testing.T) {
	engine := "00-11111111111111111111111111111111-1111111111111111-01"
	caller := "00-22222222222222222222222222222222-2222222222222222-01"
	parent := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": engine})
	input := Envelope{Version: Version, Trace: map[string]string{"traceparent": caller}}
	ctx, cancel := Incoming(parent, input)
	defer cancel()
	if !trace.SpanContextFromContext(ctx).Equal(trace.SpanContextFromContext(parent)) {
		t.Fatal("input replaced engine parent span")
	}
	ctx, stop := Incoming(context.Background(), input)
	defer stop()
	if trace.SpanContextFromContext(ctx).TraceID().String() != "22222222222222222222222222222222" {
		t.Fatal("missing execution trace did not use input fallback")
	}
}
