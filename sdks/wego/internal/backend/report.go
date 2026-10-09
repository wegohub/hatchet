package backend

import (
	"encoding/json"
	"time"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// reportType 与正式客户端的枚举转换一致，尺寸检查覆盖实际发送的事件类型
func reportType(kind v0.ActionEventType) dispatcherpb.StepActionEventType {
	switch kind {
	case v0.ActionEventTypeStarted:
		return dispatcherpb.StepActionEventType_STEP_EVENT_TYPE_STARTED
	case v0.ActionEventTypeCompleted:
		return dispatcherpb.StepActionEventType_STEP_EVENT_TYPE_COMPLETED
	case v0.ActionEventTypeFailed:
		return dispatcherpb.StepActionEventType_STEP_EVENT_TYPE_FAILED
	default:
		return dispatcherpb.StepActionEventType_STEP_EVENT_TYPE_UNKNOWN
	}
}

// reportFailure 把无法传输的业务结果变为小型不可重试失败，不能让运行一直占槽
func reportFailure(code codes.Code) (any, *bool) {
	noRetry := true
	return wire.EncodeError(status.Error(code, "wego: task result cannot be encoded within backend report budget")), &noRetry
}

// boundedStepReport 检查完整正式请求；不修改调用方持有的事件容器或执行身份
func boundedStepReport(event *v0.ActionEvent, limit int) (*v0.ActionEvent, error) {
	if event == nil || event.Action == nil {
		return nil, status.Error(codes.InvalidArgument, "wego: missing task report identity")
	}
	copy := *event
	if copy.EventTimestamp == nil {
		now := time.Now()
		copy.EventTimestamp = &now
	}
	for attempt := 0; attempt < 2; attempt++ {
		payload, err := json.Marshal(copy.EventPayload)
		code := codes.InvalidArgument
		if err == nil {
			request := &dispatcherpb.StepActionEvent{WorkerId: copy.WorkerId, JobId: copy.JobId, JobRunId: copy.JobRunId, TaskId: copy.StepId, TaskRunExternalId: copy.StepRunId, ActionId: copy.ActionId, EventPayload: string(payload), RetryCount: &copy.RetryCount, ShouldNotRetry: copy.ShouldNotRetry, EventType: reportType(copy.EventType), EventTimestamp: timestamppb.New(*copy.EventTimestamp)}
			if proto.Size(request) <= limit {
				return &copy, nil
			}
			code = codes.ResourceExhausted
		}
		if attempt == 1 || copy.EventType != v0.ActionEventTypeCompleted && copy.EventType != v0.ActionEventTypeFailed {
			return nil, status.Error(code, "wego: task report identity exceeds backend budget")
		}
		copy.EventType = v0.ActionEventTypeFailed
		copy.EventPayload, copy.ShouldNotRetry = reportFailure(code)
	}
	panic("unreachable report bound")
}

// boundedBatchReports 在发送前完成全部校验；按正式 protobuf 尺寸拆分，不按 JSON 估算
// 例如三条合法结果合计超限时拆成多个 COMPLETED；单条超限只把该成员改为 FAILED
func boundedBatchReports(event *v0.BatchActionEvent, limit int) ([]*v0.BatchActionEvent, error) {
	if event == nil || len(event.Items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "wego: empty batch report")
	}
	base := *event
	if base.EventTimestamp == nil {
		now := time.Now()
		base.EventTimestamp = &now
	}
	// chunks 在全部校验完成后才交给发送方，避免后项非法导致部分任务已经上报
	var chunks []*v0.BatchActionEvent
	// current 保存相同事件类型且尚未达到完整 RPC 预算的分块
	var current *v0.BatchActionEvent
	for _, original := range event.Items {
		if original == nil {
			return nil, status.Error(codes.InvalidArgument, "wego: nil batch report member")
		}
		item, kind := *original, event.EventType
		single := base
		for attempt := 0; attempt < 2; attempt++ {
			single.EventType, single.Items = kind, []*v0.BatchActionEventItem{&item}
			size, err := batchReportSize(&single)
			if err == nil && size <= limit {
				break
			}
			code := codes.ResourceExhausted
			if err != nil {
				code = codes.InvalidArgument
			}
			if attempt == 1 || kind != v0.ActionEventTypeCompleted && kind != v0.ActionEventTypeFailed {
				return nil, status.Error(code, "wego: batch report identity exceeds backend budget")
			}
			kind = v0.ActionEventTypeFailed
			item.EventPayload, item.ShouldNotRetry = reportFailure(code)
		}
		if current != nil && current.EventType == kind {
			candidate := *current
			candidate.Items = append(append([]*v0.BatchActionEventItem(nil), current.Items...), &item)
			size, _ := batchReportSize(&candidate)
			if size <= limit {
				current.Items = candidate.Items
				continue
			}
		}
		current = &single
		chunks = append(chunks, current)
	}
	return chunks, nil
}

// batchReportSize 包含 envelope、每条 payload、retry 和不可重试标记的实际传输大小
func batchReportSize(event *v0.BatchActionEvent) (int, error) {
	request := &dispatcherpb.BatchActionEvent{WorkerId: event.WorkerId, JobId: event.JobId, ActionId: event.ActionId, BatchId: event.BatchId, EventType: reportType(event.EventType), EventTimestamp: timestamppb.New(*event.EventTimestamp)}
	for _, item := range event.Items {
		payload, err := json.Marshal(item.EventPayload)
		if err != nil {
			return 0, err
		}
		request.Items = append(request.Items, &dispatcherpb.BatchActionEventItem{TaskRunExternalId: item.TaskRunExternalId, EventPayload: string(payload), RetryCount: item.RetryCount, ShouldNotRetry: item.ShouldNotRetry})
	}
	return proto.Size(request), nil
}
