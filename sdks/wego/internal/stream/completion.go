package stream

import (
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Completion 是引擎成功结果携带的最终输出清单，不把 ATTEMPT_END 等同于任务成功
type Completion = model.StreamCompletion

// VerifyCompletion 必须在引擎权威成功后调用；delivered 不包含仅预取的消息
// 引擎成功、匹配的结束清单、无缺口输出和实际交付四者齐全才允许 EOF
func VerifyCompletion(state State, delivered uint64, final Completion) error {
	if final.Version != wire.LogVersion || final.TaskID != state.TaskID || final.Writer == "" || final.EndID == "" {
		return status.Error(codes.DataLoss, "wego: invalid final stream completion identity")
	}
	if state.End == nil || final.Epoch != state.Epoch || final.Writer != state.Writer || final.EndID != state.End.FrameId {
		return status.Error(codes.DataLoss, "wego: final engine result has no matching attempt end")
	}
	// 结束帧包含错误时，即使错误地上报成功也不能向调用方返回成功 EOF
	if len(state.End.Status) > 0 {
		// terminal 是 google.rpc.Status 的完整结果，不从文本猜测成功
		// terminal 保留 code、message 和全部 details，与引擎权威错误逐项核对
		var terminal statuspb.Status
		if err := proto.Unmarshal(state.End.Status, &terminal); err != nil || terminal.Code != 0 {
			return status.Error(codes.DataLoss, "wego: successful engine result contradicts attempt status")
		}
	}
	if final.LastOutput != state.LastOutput || state.End.OutputSeq != final.LastOutput || delivered != final.LastOutput {
		return status.Error(codes.DataLoss, "wego: final stream output is incomplete or undelivered")
	}
	return nil
}

// VerifyFailureCompletion 对最终失败执行核对同一身份、有效输出和完整 status details
// Unavailable 的业务重试结束帧不能被当作另一代次的最终失败
func VerifyFailureCompletion(state State, delivered uint64, final Completion, business error) error {
	if final.Version != wire.LogVersion || final.TaskID != state.TaskID || final.Writer == "" || final.EndID == "" || state.End == nil ||
		final.Epoch != state.Epoch || final.Writer != state.Writer || final.EndID != state.End.FrameId ||
		final.LastOutput != state.LastOutput || state.End.OutputSeq != final.LastOutput || delivered != final.LastOutput {
		return status.Error(codes.DataLoss, "wego: failed run output manifest mismatch")
	}
	// terminal 保留 code、message 和全部 details，与引擎权威错误逐项核对
	var terminal statuspb.Status
	if err := proto.Unmarshal(state.End.Status, &terminal); err != nil || terminal.Code == 0 || !proto.Equal(&terminal, status.Convert(business).Proto()) {
		return status.Error(codes.DataLoss, "wego: failed engine result contradicts attempt status")
	}
	return nil
}
