package spec

import (
	"context"
	"crypto/tls"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// TestRuntimeCloneOwnership 验证两层投影和配置容器均独立，能力对象仍共享。
func TestRuntimeCloneOwnership(t *testing.T) {
	// source 模拟客户端同时使用投影、标签、TLS、payload 与 embedded 配置。
	source := Defaults()
	source.Projections["/fixture/Call"] = map[string]string{"group": "group_key"}
	source.Labels["owner"] = "first"
	source.TLS = &tls.Config{ServerName: "first"}
	source.Embedded = &EmbeddedConfig{GRPCPort: 7077}
	source.Middleware = []middleware.Option{{}}
	source.Telemetry = telemetry.Config{Trace: telemetry.TraceConfig{Exporters: []sdktrace.SpanExporter{nil}}}
	// local 是 Worker 独立视图，修改任一容器都不能反向改变 source。
	local := source.Clone()
	local.Projections["/fixture/Call"]["group"] = "account"
	local.Projections["/fixture/New"] = map[string]string{"id": "id"}
	local.Labels["owner"] = "second"
	local.TLS.ServerName = "second"
	local.Embedded.GRPCPort = 9000
	local.Middleware[0].UnaryClient = func(_ctx context.Context, _method string, _req, _reply any, _cc *grpc.ClientConn, _invoker grpc.UnaryInvoker, _opts ...grpc.CallOption) error {
		return nil
	}
	local.Telemetry.Trace.Exporters[0] = &noopExporter{}
	if source.Projections["/fixture/Call"]["group"] != "group_key" || source.Projections["/fixture/New"] != nil || source.Labels["owner"] != "first" || source.TLS.ServerName != "first" || source.Embedded.GRPCPort != 7077 || source.Middleware[0].UnaryClient != nil || source.Telemetry.Trace.Exporters[0] != nil {
		t.Fatal("clone retained mutable container ownership")
	}
}

// noopExporter 是无外部资源的能力对象，用于确认 exporter 切片容器独立。
type noopExporter struct{}

// ExportSpans 不执行网络 I/O，此测试仅检查配置复制规则。
func (*noopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }

// Shutdown 无外部资源，按 exporter 契约返回成功。
func (*noopExporter) Shutdown(context.Context) error { return nil }
