package backend

import (
	"context"
	"strings"
	"testing"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// boundedReporter 模拟第二个分块确认丢失，不能清空还未确认的运行登记
type boundedReporter struct {
	// DispatcherClient 的其他方法不参与结果上报边界
	v0.DispatcherClient
	// batches 保存已尝试的独立结果容器
	batches []*v0.BatchActionEvent
}

// SendBatchActionEvent 第一块确认成功，第二块返回可诊断的传输错误
func (r *boundedReporter) SendBatchActionEvent(_ context.Context, event *v0.BatchActionEvent) (*v0.ActionEventResponse, error) {
	r.batches = append(r.batches, event)
	if len(r.batches) == 2 {
		return nil, status.Error(codes.Unavailable, "report ACK lost")
	}
	return &v0.ActionEventResponse{}, nil
}

// TestBoundedReportFailure 验证超大或不可序列化输出会终结运行，不覆盖业务持有的原事件
func TestBoundedReportFailure(t *testing.T) {
	for _, item := range []struct {
		// name、payload 和 code 描述编码失败或超限的不同原因
		name string
		// payload 构造超大 JSON 或不支持 JSON 的业务结果
		payload any
		// code 是传输层应保留的失败原因
		code codes.Code
	}{{"oversized", strings.Repeat("x", 4096), codes.ResourceExhausted}, {"invalid_json", make(chan int), codes.InvalidArgument}} {
		t.Run(item.name, func(t *testing.T) {
			event := &v0.ActionEvent{Action: &v0.Action{StepRunId: "task", RetryCount: 2}, EventType: v0.ActionEventTypeCompleted, EventPayload: item.payload}
			bounded, err := boundedStepReport(event, 1024)
			if err != nil || bounded.EventType != v0.ActionEventTypeFailed || bounded.ShouldNotRetry == nil || !*bounded.ShouldNotRetry {
				t.Fatal(bounded, err)
			}
			if status.Code(wire.DecodeError(bounded.EventPayload.(string))) != item.code {
				t.Fatal("failure code lost")
			}
			if event.EventType != v0.ActionEventTypeCompleted || event.ShouldNotRetry != nil || bounded.RetryCount != 2 {
				t.Fatal("original report identity or payload mutated")
			}
		})
	}
}

// TestBatchReportPartialACK 验证完整尺寸分块、单条失败隔离，以及第二块失败时保留尚未确认的 pending
func TestBatchReportPartialACK(t *testing.T) {
	reporter := &boundedReporter{}
	owner := &transport{backend: &Backend{config: spec.Runtime{BackendMessageLimit: 1024}}, pending: map[string]string{attemptKey("one", 0): "worker", attemptKey("two", 0): "worker", attemptKey("large", 0): "worker"}, reports: map[string]context.Context{}}
	d := &dispatcher{DispatcherClient: reporter, owner: owner}
	event := &v0.BatchActionEvent{WorkerId: "worker", EventType: v0.ActionEventTypeCompleted, Items: []*v0.BatchActionEventItem{{TaskRunExternalId: "one", EventPayload: strings.Repeat("x", 700)}, {TaskRunExternalId: "two", EventPayload: strings.Repeat("x", 700)}, {TaskRunExternalId: "large", EventPayload: strings.Repeat("x", 4096)}}}
	chunks, err := boundedBatchReports(event, 1024)
	if err != nil || len(chunks) != 3 || chunks[2].EventType != v0.ActionEventTypeFailed {
		t.Fatal(chunks, err)
	}
	for _, chunk := range chunks {
		size, err := batchReportSize(chunk)
		if err != nil || size > 1024 {
			t.Fatal("invalid chunk budget", size, err)
		}
	}
	if event.Items[2].ShouldNotRetry != nil {
		t.Fatal("batch fixture mutated")
	}
	if _, err := d.SendBatchActionEvent(context.Background(), event); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if len(owner.pending) != 2 || owner.pending[attemptKey("two", 0)] != "worker" || owner.pending[attemptKey("large", 0)] != "worker" {
		t.Fatal("unconfirmed report erased", owner.pending)
	}
}
