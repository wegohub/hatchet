//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"google.golang.org/grpc"
)

// consoleTraceSpan 读取 Console 实际使用的追踪 API，不根据本地 exporter 推测结果。
type consoleTraceSpan struct {
	// TraceID 是控制台当前查询链路的身份。
	TraceID string `json:"traceId"`
	// SpanID 与 ParentSpanID 用于确认业务 span 确实接续有效父链路。
	SpanID string `json:"spanId"`
	// ParentSpanID 为空说明业务重新生成了根 trace。
	ParentSpanID string `json:"parentSpanId"`
	// Kind 区分同名的客户端提交 span 与服务端 handler span。
	Kind string `json:"spanKind"`
	// Name 区分引擎排队、执行、运行根与业务方法。
	Name string `json:"spanName"`
}

// waitConsoleTrace 等待真实导出和 OLAP 入库，检查同一 API 响应包含引擎与业务 spans。
func waitConsoleTrace(t *testing.T, ctx context.Context, config spec.Runtime, runID, method string) string {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	// last 保留最近一次可读响应，超时时报告缺失的 spans。
	var last []consoleTraceSpan
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.ServerURL+"/api/v1/stable/tenants/"+config.TenantID+"/traces?run_external_id="+runID, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+config.Token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode == http.StatusOK {
			// result 保留 API 列表结构，避免从原始字符串猜测 spans。
			var result struct {
				// Rows 是控制台可见的完整 trace 列表。
				Rows []consoleTraceSpan `json:"rows"`
			}
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			last = result.Rows
			names := make(map[string]bool)
			traceID := ""
			for _, span := range last {
				if traceID == "" {
					traceID = span.TraceID
				}
				if traceID != span.TraceID {
					t.Fatal("Console returned mixed trace IDs")
				}
				if span.Name == method && span.Kind == "SERVER" && span.ParentSpanID == "" {
					t.Fatal("handler trace has no parent")
				}
				if span.Name != method || span.Kind == "SERVER" {
					names[span.Name] = true
				}
			}
			if names[method] && names["hatchet.engine.workflow_run"] && names["hatchet.engine.queued"] && names["hatchet.engine.start_step_run"] {
				t.Logf("run_id=%s trace_id=%s spans=%d", runID, traceID, len(last))
				return traceID
			}
		} else if response.StatusCode != http.StatusNotFound {
			t.Fatalf("trace API status=%d", response.StatusCode)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("trace incomplete: run=%s spans=%v error=%v", runID, last, ctx.Err())
		case <-ticker.C:
		}
	}
}

// TestConsoleTraceLinkage 验证生成调用、可靠流、无调用者 span 的 Run、Console 和事件共享引擎 trace。
func TestConsoleTraceLinkage(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service := &eventService{executions: make(chan eventExecution, 8)}
	h, err := scenarios.StartRegistered(ctx, "trace_linkage", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, service) }, streamNames(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	observer := nativeObserver(t, h)
	rpc := pb.NewGreeterClient(h.Conn)
	cases := []struct {
		// name 与 method 描述本次提交入口及真正的 handler。
		name string
		// method 是控制台中业务 span 的预期名称。
		method string
		// invoke 完成一次调用或事件发布，不伪造运行身份。
		invoke func() error
	}{
		{"unary", pb.Greeter_SayHello_FullMethodName, func() error { _, err := rpc.SayHello(ctx, &pb.Request{Message: "trace"}); return err }},
		{"reliable", pb.Greeter_WatchHellos_FullMethodName, func() error {
			stream, err := rpc.WatchHellos(ctx, &pb.Request{Count: 1})
			if err != nil {
				return err
			}
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		}},
		{"run_no_parent", pb.Greeter_SayHello_FullMethodName, func() error {
			ref, err := h.Conn.RunNoWait(ctx, pb.Greeter_SayHello_FullMethodName, &pb.Request{Message: "trace"})
			if err != nil {
				return err
			}
			_, err = ref.Result(ctx)
			return err
		}},
		{"console", pb.Greeter_SayHello_FullMethodName, func() error {
			nativeTrigger(t, ctx, observer, pb.Greeter_SayHello_FullMethodName, map[string]any{"message": "trace"})
			return nil
		}},
		{"event", pb.Greeter_SayHello_FullMethodName, func() error {
			return h.Conn.Events().Push(ctx, pb.Greeter_SayHello_FullMethodName, &pb.Request{Message: "trace"})
		}},
		{"event_reliable", pb.Greeter_WatchHellos_FullMethodName, func() error {
			return h.Conn.Events().Push(ctx, pb.Greeter_WatchHellos_FullMethodName, &pb.Request{Count: 1})
		}},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			if err := item.invoke(); err != nil {
				t.Fatal(err)
			}
			select {
			case executed := <-service.executions:
				if executed.method != item.method {
					t.Fatal(fmt.Sprintf("unexpected execution: %v", executed))
				}
				traceID := waitConsoleTrace(t, ctx, observer.Config(), executed.runID, item.method)
				h.Report.AddTrace(h, item.name+": Console contains engine and handler spans with common trace ID", &pb.Reply{RunId: executed.runID}, traceID)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
