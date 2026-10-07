package backend

import (
	"context"
	"fmt"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// submitChildren 在 SDK 内分块并累计确认身份，兼容未分块和分块的官方 listener。
// 例如 101 项中前 100 项获确认、最后一项超时，仍返回前 100 个句柄和部分提交错误。
func (e *execution) submitChildren(ctx context.Context, name string, requests []*v1.TriggerWorkflowRequest) ([]ports.Run, error) {
	// refs 只包含引擎明确确认的身份，不猜测超时块是否接受。
	refs := make([]ports.Run, 0, len(requests))
	// fail 保留成功输入的位置及 RunID，调用方可查询而无需重复提交整批。
	fail := func(err error) ([]ports.Run, error) {
		if len(refs) == 0 {
			return refs, err
		}
		// ids 的顺序与成功输入下标一致。
		ids := make([]string, len(refs))
		// i 和 ref 按已确认的输入顺序提取 RunID。
		for i, ref := range refs {
			ids[i] = ref.ID
		}
		return refs, &model.PartialSubmissionError{SuccessfulRunIDs: ids, Err: err}
	}
	// 编码上限在首次网络提交前检查，单项无法发送时不能留下半批执行。
	for _, request := range requests {
		if proto.Size(request) > 3<<20 {
			return fail(fmt.Errorf("wego: durable child request exceeds submission limit"))
		}
	}
	// start 保留原输入下标，每次只推进已经完整确认的一块。
	for start := 0; start < len(requests); {
		// end 同时约束数量和字节；最多 100 项，预留传输字段开销。
		end, size := start+1, proto.Size(requests[start])
		for end < len(requests) && end-start < 100 && size+proto.Size(requests[end])+8 <= 3<<20 {
			size += proto.Size(requests[end]) + 8
			end++
		}
		// response 通过统一 ACK 槽提交一块，后续块失败不能清空已确认身份。
		response, err := e.eventAck(ctx, &v1.DurableTaskRequest{Message: &v1.DurableTaskRequest_TriggerRuns{
			TriggerRuns: &v1.DurableTaskTriggerRunsRequest{
				DurableTaskExternalId: e.original.StepRunId(), InvocationCount: int32(e.Info().InvocationCount), TriggerOpts: requests[start:end],
			},
		}})
		if err != nil {
			return fail(err)
		}
		// ack 必须与请求类型一致，缺失确认不能按零项成功处理。
		ack := response.GetTriggerRunsAck()
		if ack == nil {
			return fail(fmt.Errorf("wego: child submission acknowledgment missing"))
		}
		// i 对应块内输入位置，entry 必须含引擎确认的真实 RunID。
		for i, entry := range ack.RunEntries {
			if i >= end-start || entry == nil || entry.WorkflowRunExternalId == "" {
				return fail(fmt.Errorf("wego: invalid durable child identity"))
			}
			// ref 关联当前块的原输入下标，例如第二块首项仍为 100。
			ref := e.childRef(v0.TriggerRunAckEntry{WorkflowRunID: entry.WorkflowRunExternalId, BranchID: entry.BranchId, NodeID: entry.NodeId}, name)
			ref.InputIndex = start + i
			refs = append(refs, ref)
		}
		if len(ack.RunEntries) != end-start {
			return fail(fmt.Errorf("wego: durable child result count differs"))
		}
		start = end
	}
	return refs, nil
}
