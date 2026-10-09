package engine

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// TestSpanUsesActualProtocolVersion 验证 trace 属性与真实 wire 协议一致
func TestSpanUsesActualProtocolVersion(t *testing.T) {
	// exporter 由测试拥有，实例关闭只能 flush，不能清空其他使用者的出口
	exporter := tracetest.NewInMemoryExporter()
	defer exporter.Shutdown(context.Background())
	// config 禁止外部出口，追踪只进入受控内存 exporter
	config := spec.Defaults()
	config.Telemetry.Trace.DisableWorkerExporter = true
	config.Telemetry.Trace.Exporters = []sdktrace.SpanExporter{exporter}
	// instance 完整创建 provider 和生命周期，后端不连接真实引擎
	instance, err := NewWithBackend(config, &closingBackend{})
	if err != nil {
		t.Fatal(err)
	}
	// finish 发布真实 span，不把常量本身的比较当作观测行为验证
	_, finish := instance.StartSpan(context.Background(), "/fixture/Call", trace.SpanKindClient)
	finish(nil)
	// err 关闭时 flush span，测试不靠 exporter 轮询周期猜测完成
	if err = instance.Close(); err != nil {
		t.Fatal(err)
	}
	// spans 是实际导出的快照，必须只有当前调用的一个 span
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span count %d", len(spans))
	}
	// attribute 检查已经导出的属性值，协议升级时测试会随 wire.Version 保持准确
	for _, attribute := range spans[0].Attributes {
		if string(attribute.Key) == "wego.protocol.version" {
			if attribute.Value.AsInt64() != int64(wire.Version) {
				t.Fatalf("wrong protocol attribute: %v", attribute)
			}
			return
		}
	}
	t.Fatal("protocol attribute missing")
}
