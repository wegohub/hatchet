package telemetry

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	public "github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// TestProtocolMetricsInstanceBoundary 验证指标可抓取、默认关闭、实例隔离和有限标签，不替换全局 trace provider
func TestProtocolMetricsInstanceBoundary(t *testing.T) {
	global := otel.GetTracerProvider()
	r, err := New(context.Background(), public.Config{Trace: public.TraceConfig{DisableWorkerExporter: true}, Metrics: public.MetricsConfig{Enabled: true, Addr: "127.0.0.1:0"}}, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for name := range r.protocol {
		r.ObserveProtocol(ports.ProtocolMetric{Name: name, Method: "/fixture.Service/Watch", Mode: "reliable", Outcome: "success", Value: 1})
	}
	// HTTP 返回必须包含所有协议指标，且身份不能成为时间序列标签
	response, err := http.Get("http://" + r.listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	for name := range r.protocol {
		if !strings.Contains(string(data), name+"{") && !strings.Contains(string(data), name+"_count{") {
			t.Fatal("missing protocol metric", name)
		}
	}
	for _, identity := range []string{"run_id=", "worker_key=", "writer=", "group="} {
		if strings.Contains(string(data), identity) {
			t.Fatal("unbounded identity label", identity)
		}
	}
	if len(r.protocol) != 16 || otel.GetTracerProvider() != global {
		t.Fatal("metrics or global provider boundary changed")
	}
	disabled, err := New(context.Background(), public.Config{Trace: public.TraceConfig{DisableWorkerExporter: true}}, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close(context.Background())
	disabled.ObserveProtocol(ports.ProtocolMetric{Name: "wego_stream_publish_total", Value: 1})
	if disabled.protocol != nil || disabled.listener != nil {
		t.Fatal("disabled metrics allocated resources")
	}
}
